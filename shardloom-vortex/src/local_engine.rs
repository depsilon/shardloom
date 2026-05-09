#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity,
    PredicateExpr, Result, ShardLoomError, StatValue,
};
use shardloom_plan::ProjectionRequest;

use crate::{
    VortexBoundedExecutionPolicy, VortexBoundedExecutionReport, VortexBoundedExecutionStatus,
    VortexLocalExecutionReport, VortexLocalExecutionStatus, VortexLocalPrimitiveExecutionReport,
    VortexLocalPrimitiveExecutionStatus, VortexMetadataOpenReport, VortexMetadataOpenRequest,
    VortexMetadataOpenStatus, VortexMetadataProbeReport, VortexQueryPrimitiveAnalysisReport,
    VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult, VortexQueryPrimitiveStatus,
    VortexQueryPrimitiveValue, VortexWorkAvoidedMetric, VortexWorkAvoidedMetricKind,
    VortexWorkAvoidedReport, execute_vortex_bounded_local_query, execute_vortex_local_primitive,
    execute_vortex_local_query_primitive, open_vortex_metadata_only,
    summarize_vortex_metadata_probe,
};

/// Stable local-engine status for `ShardLoom` `Vortex` integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalEngineStatus {
    Planned,
    MetadataCompleted,
    LocalEncodedCountCompleted,
    LocalPrimitiveCompleted,
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
            Self::LocalEncodedCountCompleted => "local_encoded_count_completed",
            Self::LocalPrimitiveCompleted => "local_primitive_completed",
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
    LocalEncodedCount,
    LocalPrimitive,
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
            Self::LocalEncodedCount => "local_encoded_count",
            Self::LocalPrimitive => "local_primitive",
            Self::NoOp => "no_op",
            Self::PlanOnly => "plan_only",
            Self::BoundedLocal => "bounded_local",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn reads_data(&self) -> bool {
        matches!(self, Self::LocalEncodedCount | Self::LocalPrimitive)
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
    pub local_primitive_execution_report: Option<VortexLocalPrimitiveExecutionReport>,
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
        let summary = if let Some(open) = metadata_open_report.as_ref() {
            if let Some(summary) = open.metadata_summary.clone() {
                summary
            } else {
                let mut degraded = summarize_vortex_metadata_probe(
                    &VortexMetadataProbeReport::deferred_api_unclear(),
                );
                degraded.diagnostics.extend(open.diagnostics.clone());
                degraded
            }
        } else {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        };
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
        let local_primitive_execution_report =
            execute_local_primitive_when_needed(&query_request, &query_result)?;
        let status = map_status(
            metadata_open_report.as_ref(),
            local_execution_report.as_ref(),
            bounded_execution_report.as_ref(),
            local_primitive_execution_report.as_ref(),
            &query_result,
        );
        let mode = map_mode(status);
        let diagnostics = collect_report_diagnostics(
            &request,
            &query_result,
            metadata_open_report.as_ref(),
            local_execution_report.as_ref(),
            bounded_execution_report.as_ref(),
            local_primitive_execution_report.as_ref(),
        );
        let value_summary =
            local_engine_value_summary(&query_result, local_primitive_execution_report.as_ref());
        let result_known =
            local_engine_result_known(&query_result, local_primitive_execution_report.as_ref());
        let task_count = bounded_execution_report
            .as_ref()
            .map_or(0, |r| r.decisions.len());
        let decision_trace_entries = analysis_report
            .as_ref()
            .map_or(0, |r| r.decision_trace.entry_count());
        let effects = VortexLocalEngineEffectSummary::from_reports(
            local_execution_report.as_ref(),
            bounded_execution_report.as_ref(),
            local_primitive_execution_report.as_ref(),
        );
        let work_avoided_metrics = runtime_work_avoided_metric_count(
            &query_result,
            local_primitive_execution_report.as_ref(),
            &effects,
        );
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
            local_primitive_execution_report,
            value_summary,
            result_known,
            task_count,
            decision_trace_entries,
            work_avoided_metrics,
            tasks_executed: effects.tasks_executed,
            data_read: effects.data_read,
            data_decoded: effects.data_decoded,
            data_materialized: effects.data_materialized,
            object_store_io: effects.object_store_io,
            write_io: effects.write_io,
            spill_io_performed: effects.spill_io_performed,
            external_effects_executed: effects.external_effects_executed,
            fallback_execution_allowed: effects.fallback_execution_allowed,
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
            local_primitive_execution_report: None,
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
        if let Some(report) = &self.local_primitive_execution_report {
            let _ = writeln!(out, "local primitive status: {}", report.status.as_str());
            let _ = writeln!(out, "local primitive mode: {}", report.mode.as_str());
            let _ = writeln!(out, "local primitive rows scanned: {}", report.rows_scanned);
            let _ = writeln!(
                out,
                "local primitive rows selected: {}",
                report
                    .rows_selected
                    .map_or_else(|| "none".to_string(), |value| value.to_string())
            );
            let _ = writeln!(
                out,
                "local primitive projected columns: {}",
                report.projected_columns.join(",")
            );
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
        let _ = writeln!(out, "tasks executed: {}", self.tasks_executed);
        let _ = writeln!(out, "data read: {}", self.data_read);
        let _ = writeln!(out, "data decoded: {}", self.data_decoded);
        let _ = writeln!(out, "data materialized: {}", self.data_materialized);
        let _ = writeln!(out, "object-store IO: {}", self.object_store_io);
        let _ = writeln!(out, "write IO: {}", self.write_io);
        let _ = writeln!(out, "spill IO: {}", self.spill_io_performed);
        let _ = writeln!(
            out,
            "external effects executed: {}",
            self.external_effects_executed
        );
        let _ = writeln!(out, "fallback execution disabled");
        out
    }

    pub fn runtime_work_avoided_report(&self) -> VortexWorkAvoidedReport {
        runtime_work_avoided_report(
            self.query_result.as_ref(),
            self.local_primitive_execution_report.as_ref(),
            &VortexLocalEngineEffectSummary {
                tasks_executed: self.tasks_executed,
                data_read: self.data_read,
                data_decoded: self.data_decoded,
                data_materialized: self.data_materialized,
                object_store_io: self.object_store_io,
                write_io: self.write_io,
                spill_io_performed: self.spill_io_performed,
                external_effects_executed: self.external_effects_executed,
                fallback_execution_allowed: self.fallback_execution_allowed,
            },
        )
    }
}

fn runtime_work_avoided_report(
    query_result: Option<&VortexQueryPrimitiveResult>,
    local_primitive: Option<&VortexLocalPrimitiveExecutionReport>,
    effects: &VortexLocalEngineEffectSummary,
) -> VortexWorkAvoidedReport {
    let mut work = VortexWorkAvoidedReport::empty();
    work.data_read = effects.data_read;
    work.data_decoded = effects.data_decoded;
    work.data_materialized = effects.data_materialized;
    work.object_store_io = effects.object_store_io;
    work.write_io = effects.write_io;
    work.spill_io_performed = effects.spill_io_performed;
    work.fallback_execution_allowed = effects.fallback_execution_allowed;
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::DecodeAvoided,
        !effects.data_decoded,
        "final local-engine effects indicate whether decode was avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::MaterializationAvoided,
        !effects.data_materialized,
        "final local-engine effects indicate whether materialization was avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::ObjectStoreRequestsAvoided,
        !effects.object_store_io,
        "final local-engine effects indicate whether object-store requests were avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::SpillAvoided,
        !effects.spill_io_performed,
        "final local-engine effects indicate whether spill IO was avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::FallbackBlocked,
        !effects.fallback_execution_allowed,
        "fallback remains disabled by policy",
    ));
    append_runtime_rows_not_scanned_metric(&mut work, query_result, local_primitive);
    work.add_metric(VortexWorkAvoidedMetric::unknown(
        VortexWorkAvoidedMetricKind::SegmentsPruned,
        "runtime segment prune count is not yet available from the local primitive path",
    ));
    work.add_metric(VortexWorkAvoidedMetric::unknown(
        VortexWorkAvoidedMetricKind::BytesNotRead,
        "runtime bytes-not-read is unknown until safe source byte accounting lands",
    ));
    work
}

fn runtime_work_avoided_metric_count(
    query_result: &VortexQueryPrimitiveResult,
    local_primitive: Option<&VortexLocalPrimitiveExecutionReport>,
    effects: &VortexLocalEngineEffectSummary,
) -> usize {
    runtime_work_avoided_report(Some(query_result), local_primitive, effects).metric_count()
}

fn append_runtime_rows_not_scanned_metric(
    work: &mut VortexWorkAvoidedReport,
    query_result: Option<&VortexQueryPrimitiveResult>,
    local_primitive: Option<&VortexLocalPrimitiveExecutionReport>,
) {
    if let Some(local) = local_primitive {
        if matches!(local.status, VortexLocalPrimitiveExecutionStatus::Executed) {
            work.add_metric(VortexWorkAvoidedMetric::known_u64(
                VortexWorkAvoidedMetricKind::RowsNotScanned,
                0,
                format!(
                    "local primitive scanned {} rows; row-skip accounting is not yet implemented for this runtime path",
                    local.rows_scanned
                ),
            ));
            return;
        }
    }
    if let Some(query_result) = query_result {
        if matches!(
            query_result.status,
            VortexQueryPrimitiveStatus::MetadataAnswered
        ) && let VortexQueryPrimitiveValue::Count(count) = query_result.value
        {
            work.add_metric(VortexWorkAvoidedMetric::known_u64(
                VortexWorkAvoidedMetricKind::RowsNotScanned,
                count,
                "metadata result avoided scanning rows at runtime",
            ));
            return;
        }
    }
    work.add_metric(VortexWorkAvoidedMetric::unknown(
        VortexWorkAvoidedMetricKind::RowsNotScanned,
        "runtime rows-not-scanned is unknown for this status",
    ));
}

fn execute_local_primitive_when_needed(
    query_request: &VortexQueryPrimitiveRequest,
    query_result: &VortexQueryPrimitiveResult,
) -> Result<Option<VortexLocalPrimitiveExecutionReport>> {
    if query_result.value.is_known() {
        return Ok(None);
    }
    let report = execute_vortex_local_primitive(query_request)?;
    if matches!(
        report.status,
        VortexLocalPrimitiveExecutionStatus::FeatureDisabled
    ) {
        Ok(None)
    } else {
        Ok(Some(report))
    }
}

fn collect_report_diagnostics(
    request: &VortexLocalEngineRequest,
    query_result: &VortexQueryPrimitiveResult,
    metadata_open_report: Option<&VortexMetadataOpenReport>,
    local_execution_report: Option<&VortexLocalExecutionReport>,
    bounded_execution_report: Option<&VortexBoundedExecutionReport>,
    local_primitive_execution_report: Option<&VortexLocalPrimitiveExecutionReport>,
) -> Vec<Diagnostic> {
    let mut diagnostics = request.diagnostics.clone();
    diagnostics.extend(query_result.diagnostics.clone());
    if let Some(report) = metadata_open_report {
        if metadata_open_diagnostics_are_blocking(local_primitive_execution_report, report) {
            diagnostics.extend(report.diagnostics.clone());
        }
    }
    if let Some(report) = local_execution_report {
        diagnostics.extend(report.diagnostics.clone());
    }
    if let Some(report) = bounded_execution_report {
        diagnostics.extend(report.diagnostics.clone());
    }
    if let Some(report) = local_primitive_execution_report {
        diagnostics.extend(report.diagnostics.clone());
    }
    diagnostics
}

fn local_engine_value_summary(
    query_result: &VortexQueryPrimitiveResult,
    local_primitive_execution_report: Option<&VortexLocalPrimitiveExecutionReport>,
) -> Option<String> {
    if query_result.value.is_known() {
        Some(query_result.value.as_str())
    } else {
        local_primitive_execution_report.and_then(|report| report.result_summary.clone())
    }
}

fn local_engine_result_known(
    query_result: &VortexQueryPrimitiveResult,
    local_primitive_execution_report: Option<&VortexLocalPrimitiveExecutionReport>,
) -> bool {
    query_result.value.is_known()
        || local_primitive_execution_report.is_some_and(|report| {
            matches!(report.status, VortexLocalPrimitiveExecutionStatus::Executed)
                && report.result_summary.is_some()
        })
}

fn map_mode(status: VortexLocalEngineStatus) -> VortexLocalEngineMode {
    match status {
        VortexLocalEngineStatus::MetadataCompleted => VortexLocalEngineMode::MetadataOnly,
        VortexLocalEngineStatus::LocalEncodedCountCompleted => {
            VortexLocalEngineMode::LocalEncodedCount
        }
        VortexLocalEngineStatus::LocalPrimitiveCompleted => VortexLocalEngineMode::LocalPrimitive,
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
    local_primitive: Option<&VortexLocalPrimitiveExecutionReport>,
    query: &VortexQueryPrimitiveResult,
) -> VortexLocalEngineStatus {
    if let Some(status) = metadata_open_report.and_then(map_metadata_open_status) {
        return status;
    }
    if let Some(report) = local_primitive {
        if let Some(status) = map_local_primitive_status(report.status) {
            return status;
        }
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
fn map_local_primitive_status(
    status: VortexLocalPrimitiveExecutionStatus,
) -> Option<VortexLocalEngineStatus> {
    match status {
        VortexLocalPrimitiveExecutionStatus::Executed => {
            Some(VortexLocalEngineStatus::LocalPrimitiveCompleted)
        }
        VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedInput
        | VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive
        | VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedDType
        | VortexLocalPrimitiveExecutionStatus::Unsupported => {
            Some(VortexLocalEngineStatus::Unsupported)
        }
        VortexLocalPrimitiveExecutionStatus::FeatureDisabled => None,
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
        VortexBoundedExecutionStatus::BlockedByMemoryPolicy => {
            VortexLocalEngineStatus::BlockedByMemoryPolicy
        }
        VortexBoundedExecutionStatus::BlockedByMissingEstimate => {
            VortexLocalEngineStatus::MissingMetadata
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
        VortexLocalExecutionStatus::LocalEncodedCountExecuted => {
            VortexLocalEngineStatus::LocalEncodedCountCompleted
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct VortexLocalEngineEffectSummary {
    tasks_executed: bool,
    data_read: bool,
    data_decoded: bool,
    data_materialized: bool,
    object_store_io: bool,
    write_io: bool,
    spill_io_performed: bool,
    external_effects_executed: bool,
    fallback_execution_allowed: bool,
}
impl VortexLocalEngineEffectSummary {
    fn from_reports(
        local: Option<&VortexLocalExecutionReport>,
        bounded: Option<&VortexBoundedExecutionReport>,
        local_primitive: Option<&VortexLocalPrimitiveExecutionReport>,
    ) -> Self {
        Self {
            tasks_executed: local.is_some_and(|r| r.tasks_executed)
                || bounded.is_some_and(|r| r.tasks_executed)
                || local_primitive
                    .is_some_and(|r| r.status == VortexLocalPrimitiveExecutionStatus::Executed),
            data_read: local.is_some_and(|r| r.data_read)
                || bounded.is_some_and(|r| r.data_read)
                || local_primitive.is_some_and(|r| r.data_read),
            data_decoded: local.is_some_and(|r| r.data_decoded)
                || bounded.is_some_and(|r| r.data_decoded)
                || local_primitive.is_some_and(|r| r.data_decoded),
            data_materialized: local.is_some_and(|r| r.data_materialized)
                || bounded.is_some_and(|r| r.data_materialized)
                || local_primitive.is_some_and(|r| r.data_materialized),
            object_store_io: local.is_some_and(|r| r.object_store_io)
                || bounded.is_some_and(|r| r.object_store_io)
                || local_primitive.is_some_and(|r| r.object_store_io),
            write_io: local.is_some_and(|r| r.write_io)
                || bounded.is_some_and(|r| r.write_io)
                || local_primitive.is_some_and(|r| r.write_io),
            spill_io_performed: local.is_some_and(|r| r.spill_io_performed)
                || bounded.is_some_and(|r| r.spill_io_performed)
                || local_primitive.is_some_and(|r| r.spill_io_performed),
            external_effects_executed: local.is_some_and(|r| r.external_effects_executed)
                || bounded.is_some_and(|r| r.external_effects_executed)
                || local_primitive.is_some_and(|r| r.external_effects_executed),
            fallback_execution_allowed: local.is_some_and(|r| r.fallback_execution_allowed)
                || bounded.is_some_and(|r| r.fallback_execution_allowed)
                || local_primitive.is_some_and(|r| r.fallback_execution_allowed),
        }
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
        || report.metadata_open_report.as_ref().is_some_and(|r| {
            metadata_open_diagnostics_are_blocking(
                report.local_primitive_execution_report.as_ref(),
                r,
            ) && diagnostics_have_errors(&r.diagnostics)
        })
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
        || report
            .local_primitive_execution_report
            .as_ref()
            .is_some_and(|r| r.has_errors() || diagnostics_have_errors(&r.diagnostics))
}

fn metadata_open_diagnostics_are_blocking(
    local_primitive_execution_report: Option<&VortexLocalPrimitiveExecutionReport>,
    metadata_open_report: &VortexMetadataOpenReport,
) -> bool {
    let primitive_completed = local_primitive_execution_report.is_some_and(|report| {
        matches!(report.status, VortexLocalPrimitiveExecutionStatus::Executed)
            && report.result_summary.is_some()
    });
    !primitive_completed
        || !matches!(
            metadata_open_report.open_status,
            VortexMetadataOpenStatus::ApiDeferred | VortexMetadataOpenStatus::FeatureDisabled
        )
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
    fn metadata_local_execution_report() -> crate::VortexLocalExecutionReport {
        let request = VortexQueryPrimitiveRequest::count_all(
            DatasetUri::new("file://tmp/metadata.vortex").expect("uri"),
        );
        let result = VortexQueryPrimitiveResult::metadata_answered(
            request.clone(),
            crate::VortexQueryPrimitiveValue::Count(42),
        );
        let analysis = crate::analyze_vortex_query_primitive_result(result.clone());
        crate::VortexLocalExecutionReport::metadata_executed(
            crate::VortexLocalExecutionInput::new(request),
            result,
            analysis,
        )
    }
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
    fn effect_summary_propagates_bounded_metadata_task_execution() {
        let local = metadata_local_execution_report();
        let bounded = execute_vortex_bounded_local_query(
            local.clone(),
            VortexBoundedExecutionPolicy::new(
                shardloom_exec::MemoryBudget::from_gib(1).expect("budget"),
            ),
        )
        .expect("bounded report");
        let effects =
            VortexLocalEngineEffectSummary::from_reports(Some(&local), Some(&bounded), None);

        assert!(effects.tasks_executed);
        assert!(!effects.data_read);
        assert!(!effects.data_decoded);
        assert!(!effects.data_materialized);
        assert!(!effects.object_store_io);
        assert!(!effects.write_io);
        assert!(!effects.spill_io_performed);
        assert!(!effects.external_effects_executed);
        assert!(!effects.fallback_execution_allowed);
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
        let work = rep.runtime_work_avoided_report();
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::DecodeAvoided),
            "true"
        );
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::RowsNotScanned),
            "unknown"
        );
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

    #[cfg(feature = "vortex-local-primitives")]
    fn unique_vortex_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "shardloom-local-engine-{name}-{}-{nanos}.vortex",
            std::process::id()
        ))
    }

    #[cfg(feature = "vortex-local-primitives")]
    fn write_local_engine_struct_fixture(path: &std::path::Path) {
        use vortex::VortexSessionDefault as _;
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;
        use vortex::file::WriteOptionsSessionExt as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let array = StructArray::try_new(
            FieldNames::from(["value"]),
            vec![
                [1_u32, 2, 3, 4, 5]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .expect("struct array")
        .into_array();
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut bytes = Vec::new();
        let summary = runtime
            .block_on(
                session
                    .write_options()
                    .write(&mut bytes, array.to_array_stream()),
            )
            .expect("write vortex");
        assert_eq!(summary.row_count(), 5);
        std::fs::write(path, bytes).expect("write fixture");
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn local_engine_executes_feature_gated_count_where_primitive() {
        let path = unique_vortex_path("count-where");
        write_local_engine_struct_fixture(&path);
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexLocalEngineRequest::new(
            uri,
            VortexLocalEnginePrimitive::CountWhere("gte:value:3".to_string()),
            4,
            1,
        )
        .expect("request");

        let report = run_vortex_local_engine(request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            report.status,
            VortexLocalEngineStatus::LocalPrimitiveCompleted
        );
        assert_eq!(report.value_summary.as_deref(), Some("3"));
        assert!(report.result_known);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(!report.fallback_execution_allowed);
        assert!(report.local_primitive_execution_report.is_some());
        let work = report.runtime_work_avoided_report();
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::DecodeAvoided),
            "false"
        );
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::MaterializationAvoided),
            "false"
        );
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::RowsNotScanned),
            "0"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn local_engine_project_primitive_reports_schema_only_no_materialization() {
        let path = unique_vortex_path("project");
        write_local_engine_struct_fixture(&path);
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexLocalEngineRequest::new(
            uri,
            VortexLocalEnginePrimitive::Project("value".to_string()),
            4,
            1,
        )
        .expect("request");

        let report = run_vortex_local_engine(request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            report.status,
            VortexLocalEngineStatus::LocalPrimitiveCompleted
        );
        assert!(!report.has_errors());
        assert!(report.result_known);
        assert!(report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_execution_allowed);
        let metadata_open = report
            .metadata_open_report
            .as_ref()
            .expect("metadata open report");
        assert_eq!(
            metadata_open.open_status,
            VortexMetadataOpenStatus::ApiDeferred
        );
        assert!(
            !report
                .diagnostics
                .iter()
                .any(|diagnostic| matches!(diagnostic.code, DiagnosticCode::ConfigurationError))
        );
        let local = report
            .local_primitive_execution_report
            .as_ref()
            .expect("local primitive report");
        assert_eq!(local.projected_columns, vec!["value".to_string()]);
        assert!(!local.materialization_boundary_reported);
        let work = report.runtime_work_avoided_report();
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::DecodeAvoided),
            "true"
        );
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::MaterializationAvoided),
            "true"
        );
        assert_eq!(
            work.metric_value_summary(VortexWorkAvoidedMetricKind::RowsNotScanned),
            "0"
        );
    }
}
