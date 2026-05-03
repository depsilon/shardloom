#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity,
    PredicateExpr, Result, ShardLoomError, StatValue,
};
use shardloom_plan::ProjectionRequest;

use crate::{
    VortexBoundedExecutionPolicy, VortexBoundedExecutionReport, VortexBoundedExecutionStatus,
    VortexLocalExecutionReport, VortexLocalExecutionStatus, VortexMetadataOpenReport,
    VortexMetadataOpenRequest, VortexMetadataOpenStatus, VortexMetadataProbeReport,
    VortexQueryPrimitiveAnalysisReport, VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult,
    VortexQueryPrimitiveStatus, execute_vortex_bounded_local_query,
    execute_vortex_local_query_primitive, open_vortex_metadata_only,
    summarize_vortex_metadata_probe,
};

/// Stable local-engine status for `ShardLoom` `Vortex` integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalEngineStatus {
    Planned,
    MetadataCompleted,
    NoOpCompleted,
    DeferredEncodedRead,
    DeferredPredicateEvaluation,
    MissingMetadata,
    BlockedByMemoryPolicy,
    BlockedByScheduler,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    BlockedByExternalEffect,
    Unsupported,
}
impl VortexLocalEngineStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataCompleted => "metadata_completed",
            Self::NoOpCompleted => "no_op_completed",
            Self::DeferredEncodedRead => "deferred_encoded_read",
            Self::DeferredPredicateEvaluation => "deferred_predicate_evaluation",
            Self::MissingMetadata => "missing_metadata",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedByScheduler => "blocked_by_scheduler",
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
            Self::BlockedByMemoryPolicy
                | Self::BlockedByScheduler
                | Self::BlockedByDecodeRisk
                | Self::BlockedByMaterializationRisk
                | Self::BlockedByObjectStoreIo
                | Self::BlockedByWriteIo
                | Self::BlockedBySpillIo
                | Self::BlockedByExternalEffect
                | Self::Unsupported
        )
    }
    pub const fn completed_without_data_read(&self) -> bool {
        matches!(self, Self::MetadataCompleted | Self::NoOpCompleted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalEngineMode {
    MetadataOnly,
    NoOp,
    PlanOnly,
    BoundedLocal,
    Blocked,
    Unsupported,
}
impl VortexLocalEngineMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::NoOp => "no_op",
            Self::PlanOnly => "plan_only",
            Self::BoundedLocal => "bounded_local",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexLocalEnginePrimitive {
    Count,
    CountWhere(String),
    Project(String),
    Filter(String),
    Unsupported(String),
}
impl VortexLocalEnginePrimitive {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Count => "count",
            Self::CountWhere(_) => "count-where",
            Self::Project(_) => "project",
            Self::Filter(_) => "filter",
            Self::Unsupported(_) => "unsupported",
        }
    }
    pub fn summary(&self) -> String {
        match self {
            Self::Count => "count".to_string(),
            Self::CountWhere(v) => format!("count-where:{v}"),
            Self::Project(v) => format!("project:{v}"),
            Self::Filter(v) => format!("filter:{v}"),
            Self::Unsupported(v) => format!("unsupported:{v}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexLocalEngineRequest {
    pub uri: DatasetUri,
    pub primitive: VortexLocalEnginePrimitive,
    pub memory_gb: u64,
    pub max_parallelism: usize,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalEngineRequest {
    /// # Errors
    /// Returns an error when memory or parallelism are zero.
    pub fn new(
        uri: DatasetUri,
        primitive: VortexLocalEnginePrimitive,
        memory_gb: u64,
        max_parallelism: usize,
    ) -> Result<Self> {
        if memory_gb == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "memory_gb must be >= 1".to_string(),
            ));
        }
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be >= 1".to_string(),
            ));
        }
        Ok(Self {
            uri,
            primitive,
            memory_gb,
            max_parallelism,
            diagnostics: vec![],
        })
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
            "uri={} primitive={} memory_gb={} max_parallelism={} diagnostics={}",
            self.uri.as_str(),
            self.primitive.summary(),
            self.memory_gb,
            self.max_parallelism,
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalEngineReport {
    pub status: VortexLocalEngineStatus,
    pub mode: VortexLocalEngineMode,
    pub request: VortexLocalEngineRequest,
    pub query_request: Option<VortexQueryPrimitiveRequest>,
    pub metadata_open_report: Option<VortexMetadataOpenReport>,
    pub query_result: Option<VortexQueryPrimitiveResult>,
    pub analysis_report: Option<VortexQueryPrimitiveAnalysisReport>,
    pub local_execution_report: Option<VortexLocalExecutionReport>,
    pub bounded_execution_report: Option<VortexBoundedExecutionReport>,
    pub value_summary: Option<String>,
    pub result_known: bool,
    pub task_count: usize,
    pub decision_trace_entries: usize,
    pub work_avoided_metrics: usize,
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
impl VortexLocalEngineReport {
    /// # Errors
    /// Returns an error when nested planning/execution report creation fails.
    pub fn from_request(request: VortexLocalEngineRequest) -> Result<Self> {
        let query_request = primitive_to_query_request(&request)?;
        let metadata_open_report = open_vortex_metadata_only(
            VortexMetadataOpenRequest::metadata_only(request.uri.clone()),
        )
        .ok();
        let summary = metadata_open_report
            .as_ref()
            .and_then(|r| r.metadata_summary.clone())
            .unwrap_or_else(|| {
                summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
            });
        let query_result = crate::evaluate_vortex_query_primitive(query_request.clone(), &summary)?;
        let analysis_report = Some(crate::analyze_vortex_query_primitive_result(
            query_result.clone(),
        ));
        let local_execution_report = Some(execute_vortex_local_query_primitive(
            query_request.clone(),
            Some(summary),
        )?);
        let policy = VortexBoundedExecutionPolicy::memory_limited(
            request.memory_gb,
            request.max_parallelism,
        )?;
        let bounded_execution_report = if let Some(local_report) = local_execution_report.clone() {
            Some(execute_vortex_bounded_local_query(local_report, policy)?)
        } else {
            None
        };
        let status = map_status(
            metadata_open_report.as_ref(),
            local_execution_report.as_ref(),
            bounded_execution_report.as_ref(),
            &query_result,
        );
        let mode = map_mode(status);
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.extend(query_result.diagnostics.clone());
        if let Some(r) = &metadata_open_report {
            diagnostics.extend(r.diagnostics.clone());
        }
        if let Some(r) = &local_execution_report {
            diagnostics.extend(r.diagnostics.clone());
        }
        if let Some(r) = &bounded_execution_report {
            diagnostics.extend(r.diagnostics.clone());
        }
        let value_summary = if query_result.value.is_known() {
            Some(query_result.value.as_str())
        } else {
            None
        };
        let result_known = query_result.value.is_known();
        let task_count = bounded_execution_report
            .as_ref()
            .map_or(0, |r| r.decisions.len());
        let decision_trace_entries = analysis_report
            .as_ref()
            .map_or(0, |r| r.decision_trace.entry_count());
        let work_avoided_metrics = analysis_report
            .as_ref()
            .map_or(0, |r| r.work_avoided.metric_count());
        Ok(Self {
            status,
            mode,
            request,
            query_request: Some(query_request),
            metadata_open_report,
            query_result: Some(query_result),
            analysis_report,
            local_execution_report,
            bounded_execution_report,
            value_summary,
            result_known,
            task_count,
            decision_trace_entries,
            work_avoided_metrics,
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics,
        })
    }
    pub fn unsupported(
        request: VortexLocalEngineRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexLocalEngineStatus::Unsupported,
            mode: VortexLocalEngineMode::Unsupported,
            request,
            query_request: None,
            metadata_open_report: None,
            query_result: None,
            analysis_report: None,
            local_execution_report: None,
            bounded_execution_report: None,
            value_summary: None,
            result_known: false,
            task_count: 0,
            decision_trace_entries: 0,
            work_avoided_metrics: 0,
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
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Unsupported local-engine request.",
            Some(format!("{}. Fallback attempted: false", reason.into())),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || report_has_error_diagnostics(self)
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
        let _ = writeln!(out, "local engine status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "primitive: {}", self.request.primitive.summary());
        let _ = writeln!(
            out,
            "metadata open report present: {}",
            self.metadata_open_report.is_some()
        );
        if let Some(metadata_open_report) = &self.metadata_open_report {
            let _ = writeln!(
                out,
                "metadata open status: {}",
                metadata_open_report.open_status.as_str()
            );
            let _ = writeln!(
                out,
                "metadata open mode: {}",
                metadata_open_report.mode.as_str()
            );
            if !metadata_open_report.diagnostics.is_empty() {
                let _ = writeln!(
                    out,
                    "metadata open diagnostics: {}",
                    metadata_open_report.diagnostics.len()
                );
                for diagnostic in &metadata_open_report.diagnostics {
                    let _ = writeln!(
                        out,
                        "- [{}] {}: {}",
                        diagnostic.severity.as_str(),
                        diagnostic.code.as_str(),
                        diagnostic.message
                    );
                }
            }
        }
        if let Some(v) = &self.value_summary {
            let _ = writeln!(out, "value summary: {v}");
        }
        let _ = writeln!(out, "result known: {}", self.result_known);
        let _ = writeln!(out, "task count: {}", self.task_count);
        let _ = writeln!(
            out,
            "decision trace entries: {}",
            self.decision_trace_entries
        );
        let _ = writeln!(out, "work avoided metrics: {}", self.work_avoided_metrics);
        let _ = writeln!(out, "memory_gb: {}", self.request.memory_gb);
        let _ = writeln!(out, "max_parallelism: {}", self.request.max_parallelism);
        let _ = writeln!(out, "tasks executed: false");
        let _ = writeln!(out, "data read: false");
        let _ = writeln!(out, "data decoded: false");
        let _ = writeln!(out, "data materialized: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "write IO: false");
        let _ = writeln!(out, "spill IO: false");
        let _ = writeln!(out, "external effects executed: false");
        let _ = writeln!(out, "fallback execution disabled");
        out
    }
}

fn map_mode(status: VortexLocalEngineStatus) -> VortexLocalEngineMode {
    match status {
        VortexLocalEngineStatus::MetadataCompleted => VortexLocalEngineMode::MetadataOnly,
        VortexLocalEngineStatus::NoOpCompleted => VortexLocalEngineMode::NoOp,
        VortexLocalEngineStatus::DeferredEncodedRead
        | VortexLocalEngineStatus::DeferredPredicateEvaluation
        | VortexLocalEngineStatus::MissingMetadata
        | VortexLocalEngineStatus::Planned => VortexLocalEngineMode::PlanOnly,
        VortexLocalEngineStatus::BlockedByMemoryPolicy
        | VortexLocalEngineStatus::BlockedByScheduler
        | VortexLocalEngineStatus::BlockedByDecodeRisk
        | VortexLocalEngineStatus::BlockedByMaterializationRisk
        | VortexLocalEngineStatus::BlockedByObjectStoreIo
        | VortexLocalEngineStatus::BlockedByWriteIo
        | VortexLocalEngineStatus::BlockedBySpillIo
        | VortexLocalEngineStatus::BlockedByExternalEffect => VortexLocalEngineMode::Blocked,
        VortexLocalEngineStatus::Unsupported => VortexLocalEngineMode::Unsupported,
    }
}
fn map_status(
    metadata_open_report: Option<&VortexMetadataOpenReport>,
    local: Option<&VortexLocalExecutionReport>,
    bounded: Option<&VortexBoundedExecutionReport>,
    query: &VortexQueryPrimitiveResult,
) -> VortexLocalEngineStatus {
    if let Some(status) = metadata_open_report.and_then(map_metadata_open_status) {
        return status;
    }
    if let Some(b) = bounded {
        return map_bounded_execution_status(b.status);
    }
    if let Some(l) = local {
        return map_local_execution_status(l.status);
    }
    match query.status {
        VortexQueryPrimitiveStatus::MissingMetadata => VortexLocalEngineStatus::MissingMetadata,
        VortexQueryPrimitiveStatus::NeedsEncodedRead => {
            VortexLocalEngineStatus::DeferredEncodedRead
        }
        VortexQueryPrimitiveStatus::NeedsEncodedPredicate => {
            VortexLocalEngineStatus::DeferredPredicateEvaluation
        }
        VortexQueryPrimitiveStatus::Unsupported => VortexLocalEngineStatus::Unsupported,
        VortexQueryPrimitiveStatus::MetadataAnswered => VortexLocalEngineStatus::MetadataCompleted,
        _ => VortexLocalEngineStatus::Planned,
    }
}
fn map_metadata_open_status(report: &VortexMetadataOpenReport) -> Option<VortexLocalEngineStatus> {
    match report.open_status {
        VortexMetadataOpenStatus::InvalidTarget | VortexMetadataOpenStatus::Unsupported => {
            Some(VortexLocalEngineStatus::Unsupported)
        }
        VortexMetadataOpenStatus::FileMissing => Some(VortexLocalEngineStatus::MissingMetadata),
        VortexMetadataOpenStatus::FeatureDisabled
        | VortexMetadataOpenStatus::ApiDeferred
        | VortexMetadataOpenStatus::Planned
        | VortexMetadataOpenStatus::OpenedMetadataOnly => None,
    }
}
fn map_bounded_execution_status(status: VortexBoundedExecutionStatus) -> VortexLocalEngineStatus {
    match status {
        VortexBoundedExecutionStatus::MetadataTasksCompleted => {
            VortexLocalEngineStatus::MetadataCompleted
        }
        VortexBoundedExecutionStatus::NoOpTasksCompleted => VortexLocalEngineStatus::NoOpCompleted,
        VortexBoundedExecutionStatus::NeedsEncodedRead => {
            VortexLocalEngineStatus::DeferredEncodedRead
        }
        VortexBoundedExecutionStatus::NeedsPredicateEvaluation => {
            VortexLocalEngineStatus::DeferredPredicateEvaluation
        }
        VortexBoundedExecutionStatus::BlockedByMemoryPolicy
        | VortexBoundedExecutionStatus::BlockedByMissingEstimate => {
            VortexLocalEngineStatus::BlockedByMemoryPolicy
        }
        VortexBoundedExecutionStatus::BlockedByScheduler => {
            VortexLocalEngineStatus::BlockedByScheduler
        }
        VortexBoundedExecutionStatus::BlockedByDecodeRisk => {
            VortexLocalEngineStatus::BlockedByDecodeRisk
        }
        VortexBoundedExecutionStatus::BlockedByMaterializationRisk => {
            VortexLocalEngineStatus::BlockedByMaterializationRisk
        }
        VortexBoundedExecutionStatus::BlockedByObjectStoreIo => {
            VortexLocalEngineStatus::BlockedByObjectStoreIo
        }
        VortexBoundedExecutionStatus::BlockedByWriteIo => VortexLocalEngineStatus::BlockedByWriteIo,
        VortexBoundedExecutionStatus::BlockedBySpillIo => VortexLocalEngineStatus::BlockedBySpillIo,
        VortexBoundedExecutionStatus::BlockedByExternalEffect => {
            VortexLocalEngineStatus::BlockedByExternalEffect
        }
        VortexBoundedExecutionStatus::Unsupported => VortexLocalEngineStatus::Unsupported,
        VortexBoundedExecutionStatus::Planned
        | VortexBoundedExecutionStatus::ReadyButNoExecutableTasks => {
            VortexLocalEngineStatus::Planned
        }
    }
}
fn map_local_execution_status(status: VortexLocalExecutionStatus) -> VortexLocalEngineStatus {
    match status {
        VortexLocalExecutionStatus::MetadataExecuted => VortexLocalEngineStatus::MetadataCompleted,
        VortexLocalExecutionStatus::NoOpCompleted => VortexLocalEngineStatus::NoOpCompleted,
        VortexLocalExecutionStatus::NeedsEncodedRead => {
            VortexLocalEngineStatus::DeferredEncodedRead
        }
        VortexLocalExecutionStatus::NeedsPredicateEvaluation => {
            VortexLocalEngineStatus::DeferredPredicateEvaluation
        }
        VortexLocalExecutionStatus::MissingMetadata => VortexLocalEngineStatus::MissingMetadata,
        VortexLocalExecutionStatus::BlockedByDecodeRisk => {
            VortexLocalEngineStatus::BlockedByDecodeRisk
        }
        VortexLocalExecutionStatus::BlockedByMaterializationRisk => {
            VortexLocalEngineStatus::BlockedByMaterializationRisk
        }
        VortexLocalExecutionStatus::BlockedByObjectStoreIo => {
            VortexLocalEngineStatus::BlockedByObjectStoreIo
        }
        VortexLocalExecutionStatus::BlockedByWriteIo => VortexLocalEngineStatus::BlockedByWriteIo,
        VortexLocalExecutionStatus::BlockedBySpillIo => VortexLocalEngineStatus::BlockedBySpillIo,
        VortexLocalExecutionStatus::BlockedByExternalEffect => {
            VortexLocalEngineStatus::BlockedByExternalEffect
        }
        VortexLocalExecutionStatus::Unsupported => VortexLocalEngineStatus::Unsupported,
        VortexLocalExecutionStatus::Planned => VortexLocalEngineStatus::Planned,
    }
}

fn diagnostics_have_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics.iter().any(|d| {
        matches!(
            d.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        )
    })
}

fn report_has_error_diagnostics(report: &VortexLocalEngineReport) -> bool {
    diagnostics_have_errors(&report.diagnostics)
        || diagnostics_have_errors(&report.request.diagnostics)
        || report
            .metadata_open_report
            .as_ref()
            .is_some_and(|r| diagnostics_have_errors(&r.diagnostics))
        || report
            .query_result
            .as_ref()
            .is_some_and(|r| diagnostics_have_errors(&r.diagnostics))
        || report
            .analysis_report
            .as_ref()
            .is_some_and(|r| diagnostics_have_errors(&r.diagnostics))
        || report
            .local_execution_report
            .as_ref()
            .is_some_and(|r| diagnostics_have_errors(&r.diagnostics))
        || report
            .bounded_execution_report
            .as_ref()
            .is_some_and(|r| diagnostics_have_errors(&r.diagnostics))
}

fn primitive_to_query_request(
    request: &VortexLocalEngineRequest,
) -> Result<VortexQueryPrimitiveRequest> {
    match &request.primitive {
        VortexLocalEnginePrimitive::Count => {
            Ok(VortexQueryPrimitiveRequest::count_all(request.uri.clone()))
        }
        VortexLocalEnginePrimitive::CountWhere(pred) => {
            Ok(VortexQueryPrimitiveRequest::count_where(
                request.uri.clone(),
                parse_tiny_predicate(pred)?,
            ))
        }
        VortexLocalEnginePrimitive::Project(cols) => Ok(VortexQueryPrimitiveRequest::project(
            request.uri.clone(),
            parse_projection_columns(cols)?,
        )),
        VortexLocalEnginePrimitive::Filter(pred) => Ok(VortexQueryPrimitiveRequest::filter(
            request.uri.clone(),
            parse_tiny_predicate(pred)?,
        )),
        VortexLocalEnginePrimitive::Unsupported(raw) => {
            Ok(VortexQueryPrimitiveRequest::unsupported(
                raw.clone(),
                "local engine primitive unsupported",
            ))
        }
    }
}

fn parse_tiny_predicate(value: &str) -> Result<PredicateExpr> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["is_null", column] => Ok(PredicateExpr::IsNull {
            column: ColumnRef::new(*column)?,
        }),
        ["is_not_null", column] => Ok(PredicateExpr::IsNotNull {
            column: ColumnRef::new(*column)?,
        }),
        [op, column, int_value] => {
            let parsed: i64 = int_value.parse().map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "predicate integer literal must be valid i64".to_string(),
                )
            })?;
            let op = match *op {
                "eq" => ComparisonOp::Eq,
                "gt" => ComparisonOp::Gt,
                "gte" => ComparisonOp::GtEq,
                "lt" => ComparisonOp::Lt,
                "lte" => ComparisonOp::LtEq,
                _ => {
                    return Err(ShardLoomError::InvalidOperation(
                        "unsupported predicate operator".to_string(),
                    ));
                }
            };
            Ok(PredicateExpr::Compare {
                column: ColumnRef::new(*column)?,
                op,
                value: StatValue::Int64(parsed),
            })
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "invalid predicate format".to_string(),
        )),
    }
}

fn parse_projection_columns(value: &str) -> Result<ProjectionRequest> {
    if value == "*" {
        return Ok(ProjectionRequest::all());
    }
    let columns: std::result::Result<Vec<_>, _> = value
        .split(',')
        .map(str::trim)
        .map(ColumnRef::new)
        .collect();
    Ok(ProjectionRequest::columns(columns?))
}

/// # Errors
/// Returns an error when `input` is malformed.
pub fn parse_vortex_local_engine_primitive(input: &str) -> Result<VortexLocalEnginePrimitive> {
    if input == "count" {
        return Ok(VortexLocalEnginePrimitive::Count);
    }
    if let Some(v) = input.strip_prefix("count-where:") {
        if v.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "count-where predicate must not be empty".to_string(),
            ));
        }
        return Ok(VortexLocalEnginePrimitive::CountWhere(v.to_string()));
    }
    if let Some(v) = input.strip_prefix("project:") {
        if v.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "project columns must not be empty".to_string(),
            ));
        }
        return Ok(VortexLocalEnginePrimitive::Project(v.to_string()));
    }
    if let Some(v) = input.strip_prefix("filter:") {
        if v.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "filter predicate must not be empty".to_string(),
            ));
        }
        return Ok(VortexLocalEnginePrimitive::Filter(v.to_string()));
    }
    Err(ShardLoomError::InvalidOperation("invalid primitive; expected count, count-where:<predicate>, project:<columns>, filter:<predicate>".to_string()))
}

/// # Errors
/// Returns an error when report construction fails.
pub fn run_vortex_local_engine(
    request: VortexLocalEngineRequest,
) -> Result<VortexLocalEngineReport> {
    VortexLocalEngineReport::from_request(request)
}
pub const fn vortex_local_engine_is_side_effect_free(report: &VortexLocalEngineReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_count() {
        assert_eq!(
            parse_vortex_local_engine_primitive("count").unwrap(),
            VortexLocalEnginePrimitive::Count
        );
    }
    #[test]
    fn parses_count_where() {
        assert_eq!(
            parse_vortex_local_engine_primitive("count-where:is_null:col").unwrap(),
            VortexLocalEnginePrimitive::CountWhere("is_null:col".to_string())
        );
    }
    #[test]
    fn parses_project_star() {
        assert_eq!(
            parse_vortex_local_engine_primitive("project:*").unwrap(),
            VortexLocalEnginePrimitive::Project("*".to_string())
        );
    }
    #[test]
    fn parses_project_cols() {
        assert_eq!(
            parse_vortex_local_engine_primitive("project:col1,col2").unwrap(),
            VortexLocalEnginePrimitive::Project("col1,col2".to_string())
        );
    }
    #[test]
    fn parses_filter() {
        assert_eq!(
            parse_vortex_local_engine_primitive("filter:gt:age:30").unwrap(),
            VortexLocalEnginePrimitive::Filter("gt:age:30".to_string())
        );
    }
    #[test]
    fn rejects_malformed() {
        assert!(parse_vortex_local_engine_primitive("bogus").is_err());
    }
    #[test]
    fn request_rejects_zero_memory() {
        let uri = DatasetUri::new("file://tmp/data.vortex").unwrap();
        assert!(
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 0, 1).is_err()
        );
    }
    #[test]
    fn request_rejects_zero_parallelism() {
        let uri = DatasetUri::new("file://tmp/data.vortex").unwrap();
        assert!(
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 1, 0).is_err()
        );
    }
    #[test]
    fn status_completed_without_read() {
        assert!(VortexLocalEngineStatus::MetadataCompleted.completed_without_data_read());
    }
    #[test]
    fn deferred_not_error() {
        assert!(!VortexLocalEngineStatus::DeferredEncodedRead.is_error());
    }
    #[test]
    fn unsupported_error() {
        assert!(VortexLocalEngineStatus::Unsupported.is_error());
    }
    #[test]
    fn mode_side_effects_false() {
        let m = VortexLocalEngineMode::MetadataOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
    }
    #[test]
    fn unsupported_report_has_errors() {
        let uri = DatasetUri::new("file://tmp/data.vortex").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 1, 1).unwrap();
        let rep = VortexLocalEngineReport::unsupported(req, "x", "y");
        assert!(rep.has_errors());
        assert!(!rep.fallback_execution_allowed);
    }
    #[test]
    fn run_missing_metadata_is_safe() {
        let uri = DatasetUri::new("file:///definitely/missing.vortex").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        assert!(
            !rep.data_read
                && !rep.data_decoded
                && !rep.data_materialized
                && !rep.object_store_io
                && !rep.write_io
                && !rep.spill_io_performed
                && !rep.fallback_execution_allowed
        );
        assert!(rep.to_human_text().contains("fallback execution disabled"));
        assert!(rep.to_human_text().contains("memory_gb"));
        assert!(rep.to_human_text().contains("max_parallelism"));
        assert!(rep.to_human_text().contains("decision trace entries"));
        assert!(rep.to_human_text().contains("work avoided metrics"));
        assert!(vortex_local_engine_is_side_effect_free(&rep));
    }
    #[test]
    fn from_request_preserves_metadata_open_diag_missing_path() {
        let uri = DatasetUri::new("file:///definitely/missing.vortex").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        assert!(rep.metadata_open_report.is_some());
        assert!(!rep.fallback_execution_allowed);
    }
    #[test]
    fn from_request_preserves_metadata_open_diag_object_store_uri() {
        let uri = DatasetUri::new("s3://bucket/data.vortex").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        let open = rep.metadata_open_report.expect("metadata open report");
        assert!(
            !open.diagnostics.is_empty()
                || matches!(open.open_status, VortexMetadataOpenStatus::ApiDeferred)
        );
        assert!(!rep.fallback_execution_allowed);
    }
    #[test]
    fn from_request_preserves_metadata_open_diag_invalid_target() {
        let uri = DatasetUri::new("file://tmp/data.parquet").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        let open = rep.metadata_open_report.expect("metadata open report");
        assert_eq!(open.open_status, VortexMetadataOpenStatus::InvalidTarget);
        assert_eq!(rep.status, VortexLocalEngineStatus::Unsupported);
    }
    #[test]
    fn to_human_text_mentions_metadata_open_status_and_diagnostics() {
        let uri = DatasetUri::new("file://tmp/data.parquet").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        let text = rep.to_human_text();
        assert!(text.contains("metadata open report present: true"));
        assert!(text.contains("metadata open status:"));
        assert!(text.contains("metadata open mode:"));
    }
    #[test]
    fn has_errors_includes_metadata_open_errors() {
        let uri = DatasetUri::new("file://tmp/data.parquet").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        assert!(rep.has_errors());
    }
    #[cfg(not(feature = "vortex-file-io"))]
    #[test]
    fn from_request_preserves_feature_disabled_metadata_open_status() {
        let uri = DatasetUri::new("file:///tmp/missing.vortex").unwrap();
        let req =
            VortexLocalEngineRequest::new(uri, VortexLocalEnginePrimitive::Count, 4, 1).unwrap();
        let rep = run_vortex_local_engine(req).unwrap();
        let open = rep.metadata_open_report.expect("metadata open report");
        assert_eq!(open.open_status, VortexMetadataOpenStatus::FeatureDisabled);
    }
}
