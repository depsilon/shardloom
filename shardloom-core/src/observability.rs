//! Observability, tracing, profiling, and runtime introspection domain skeleton.
//!
//! This module defines planning/reporting types only. It does not collect metrics,
//! emit traces, execute profiling, or perform runtime effects.

use crate::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, EncodingKind,
    FallbackStatus, LogicalDType, Result, ShardLoomError,
};

fn validate_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must not be empty"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilitySurface {
    CliText,
    CliJson,
    ExplainReport,
    EstimateReport,
    DoctorReport,
    CapabilityReport,
    BenchmarkReport,
    RuntimeReport,
    OperatorProfile,
    KernelProfile,
    StructuredEvent,
    TraceSpan,
    MetricsExport,
    Unsupported,
}
impl ObservabilitySurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CliText => "cli_text",
            Self::CliJson => "cli_json",
            Self::ExplainReport => "explain_report",
            Self::EstimateReport => "estimate_report",
            Self::DoctorReport => "doctor_report",
            Self::CapabilityReport => "capability_report",
            Self::BenchmarkReport => "benchmark_report",
            Self::RuntimeReport => "runtime_report",
            Self::OperatorProfile => "operator_profile",
            Self::KernelProfile => "kernel_profile",
            Self::StructuredEvent => "structured_event",
            Self::TraceSpan => "trace_span",
            Self::MetricsExport => "metrics_export",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_machine_readable(&self) -> bool {
        matches!(
            self,
            Self::CliJson
                | Self::ExplainReport
                | Self::EstimateReport
                | Self::DoctorReport
                | Self::CapabilityReport
                | Self::BenchmarkReport
                | Self::RuntimeReport
                | Self::StructuredEvent
                | Self::TraceSpan
                | Self::MetricsExport
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SensitivityLevel {
    Public,
    Internal,
    Sensitive,
    Secret,
    Unknown,
}
impl SensitivityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Sensitive => "sensitive",
            Self::Secret => "secret",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_redaction(&self) -> bool {
        matches!(self, Self::Sensitive | Self::Secret | Self::Unknown)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactionStatus {
    NotRequired,
    Redacted,
    Omitted,
    RequiredButMissing,
    Unknown,
}
impl RedactionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Redacted => "redacted",
            Self::Omitted => "omitted",
            Self::RequiredButMissing => "required_but_missing",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_safe(&self) -> bool {
        matches!(self, Self::NotRequired | Self::Redacted | Self::Omitted)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedField {
    pub key: String,
    pub value: String,
    pub sensitivity: SensitivityLevel,
    pub redaction: RedactionStatus,
}
impl ObservedField {
    /// Creates a public/internal observed field.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `key` is empty.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        validate_non_empty("observed field key", &key)?;
        Ok(Self {
            key,
            value: value.into(),
            sensitivity: SensitivityLevel::Public,
            redaction: RedactionStatus::NotRequired,
        })
    }
    /// Creates a sensitive observed field requiring redaction.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `key` is empty.
    pub fn sensitive(key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let mut v = Self::new(key, value)?;
        v.sensitivity = SensitivityLevel::Sensitive;
        v.redaction = RedactionStatus::RequiredButMissing;
        Ok(v)
    }
    /// Creates a secret marker field without storing raw secret values.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `key` is empty.
    pub fn secret(key: impl Into<String>) -> Result<Self> {
        let mut v = Self::new(key, "[secret]")?;
        v.sensitivity = SensitivityLevel::Secret;
        v.redaction = RedactionStatus::RequiredButMissing;
        Ok(v)
    }
    #[must_use]
    pub fn redacted(mut self) -> Self {
        self.redaction = RedactionStatus::Redacted;
        self
    }
    #[must_use]
    pub fn omitted(mut self) -> Self {
        self.redaction = RedactionStatus::Omitted;
        self
    }
    #[must_use]
    pub fn is_safe_to_emit(&self) -> bool {
        if self.sensitivity.requires_redaction() {
            self.redaction.is_safe()
        } else {
            true
        }
    }
    #[must_use]
    pub fn safe_value(&self) -> String {
        match self.redaction {
            RedactionStatus::Redacted => "[redacted]".to_string(),
            RedactionStatus::Omitted => "[omitted]".to_string(),
            _ => {
                if self.sensitivity.requires_redaction() && !self.redaction.is_safe() {
                    "[unsafe]".to_string()
                } else {
                    self.value.clone()
                }
            }
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}={} ({}/{})",
            self.key,
            self.safe_value(),
            self.sensitivity.as_str(),
            self.redaction.as_str()
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricCategory {
    Planning,
    VortexScan,
    ObjectStore,
    Execution,
    Memory,
    Spill,
    Translation,
    Output,
    ExternalEffect,
    Benchmark,
    Diagnostics,
    Unsupported,
}
impl MetricCategory {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::VortexScan => "vortex_scan",
            Self::ObjectStore => "object_store",
            Self::Execution => "execution",
            Self::Memory => "memory",
            Self::Spill => "spill",
            Self::Translation => "translation",
            Self::Output => "output",
            Self::ExternalEffect => "external_effect",
            Self::Benchmark => "benchmark",
            Self::Diagnostics => "diagnostics",
            Self::Unsupported => "unsupported",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    PlanningDurationMillis,
    OptimizationDurationMillis,
    FilesConsidered,
    SegmentsConsidered,
    SegmentsPruned,
    SegmentsMetadataAnswered,
    SegmentsRead,
    ByteRangesPlanned,
    ByteRangesRead,
    BytesRead,
    BytesDecoded,
    BytesWritten,
    RowsScanned,
    RowsMaterialized,
    ObjectStoreRequests,
    ObjectStoreRetries,
    OperatorDurationMillis,
    MemoryReservedBytes,
    PeakMemoryBytes,
    SpillBytes,
    SpillFilesCreated,
    OutputBytes,
    OutputFiles,
    EffectCallsPlanned,
    EffectCallsExecuted,
    DiagnosticsCount,
    UnsupportedFeatures,
}
impl MetricKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanningDurationMillis => "planning_duration_millis",
            Self::OptimizationDurationMillis => "optimization_duration_millis",
            Self::FilesConsidered => "files_considered",
            Self::SegmentsConsidered => "segments_considered",
            Self::SegmentsPruned => "segments_pruned",
            Self::SegmentsMetadataAnswered => "segments_metadata_answered",
            Self::SegmentsRead => "segments_read",
            Self::ByteRangesPlanned => "byte_ranges_planned",
            Self::ByteRangesRead => "byte_ranges_read",
            Self::BytesRead => "bytes_read",
            Self::BytesDecoded => "bytes_decoded",
            Self::BytesWritten => "bytes_written",
            Self::RowsScanned => "rows_scanned",
            Self::RowsMaterialized => "rows_materialized",
            Self::ObjectStoreRequests => "object_store_requests",
            Self::ObjectStoreRetries => "object_store_retries",
            Self::OperatorDurationMillis => "operator_duration_millis",
            Self::MemoryReservedBytes => "memory_reserved_bytes",
            Self::PeakMemoryBytes => "peak_memory_bytes",
            Self::SpillBytes => "spill_bytes",
            Self::SpillFilesCreated => "spill_files_created",
            Self::OutputBytes => "output_bytes",
            Self::OutputFiles => "output_files",
            Self::EffectCallsPlanned => "effect_calls_planned",
            Self::EffectCallsExecuted => "effect_calls_executed",
            Self::DiagnosticsCount => "diagnostics_count",
            Self::UnsupportedFeatures => "unsupported_features",
        }
    }
    #[must_use]
    pub const fn category(&self) -> MetricCategory {
        match self {
            Self::PlanningDurationMillis | Self::OptimizationDurationMillis => {
                MetricCategory::Planning
            }
            Self::FilesConsidered
            | Self::SegmentsConsidered
            | Self::SegmentsPruned
            | Self::SegmentsMetadataAnswered
            | Self::SegmentsRead
            | Self::ByteRangesPlanned
            | Self::ByteRangesRead
            | Self::BytesRead
            | Self::BytesDecoded => MetricCategory::VortexScan,
            Self::ObjectStoreRequests | Self::ObjectStoreRetries => MetricCategory::ObjectStore,
            Self::RowsScanned | Self::RowsMaterialized | Self::OperatorDurationMillis => {
                MetricCategory::Execution
            }
            Self::MemoryReservedBytes | Self::PeakMemoryBytes => MetricCategory::Memory,
            Self::SpillBytes | Self::SpillFilesCreated => MetricCategory::Spill,
            Self::BytesWritten => MetricCategory::Translation,
            Self::OutputBytes | Self::OutputFiles => MetricCategory::Output,
            Self::EffectCallsPlanned | Self::EffectCallsExecuted => MetricCategory::ExternalEffect,
            Self::DiagnosticsCount => MetricCategory::Diagnostics,
            Self::UnsupportedFeatures => MetricCategory::Unsupported,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricUnit {
    Count,
    Bytes,
    Milliseconds,
    Rows,
    Files,
    Segments,
    Requests,
    Calls,
    Unknown,
}
impl MetricUnit {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Bytes => "bytes",
            Self::Milliseconds => "milliseconds",
            Self::Rows => "rows",
            Self::Files => "files",
            Self::Segments => "segments",
            Self::Requests => "requests",
            Self::Calls => "calls",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ObservabilityMetricValue {
    U64(u64),
    F64(f64),
    Unknown,
}
impl ObservabilityMetricValue {
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        match self {
            Self::U64(v) => v.to_string(),
            Self::F64(v) => format!("{v:.3}"),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub kind: MetricKind,
    pub unit: MetricUnit,
    pub value: ObservabilityMetricValue,
    pub fields: Vec<ObservedField>,
}
impl MetricSample {
    #[must_use]
    pub fn new(kind: MetricKind, value: ObservabilityMetricValue) -> Self {
        let unit = match kind {
            MetricKind::BytesRead
            | MetricKind::BytesDecoded
            | MetricKind::BytesWritten
            | MetricKind::MemoryReservedBytes
            | MetricKind::PeakMemoryBytes
            | MetricKind::SpillBytes
            | MetricKind::OutputBytes => MetricUnit::Bytes,
            MetricKind::PlanningDurationMillis
            | MetricKind::OptimizationDurationMillis
            | MetricKind::OperatorDurationMillis => MetricUnit::Milliseconds,
            MetricKind::RowsScanned | MetricKind::RowsMaterialized => MetricUnit::Rows,
            MetricKind::FilesConsidered
            | MetricKind::SpillFilesCreated
            | MetricKind::OutputFiles => MetricUnit::Files,
            MetricKind::SegmentsConsidered
            | MetricKind::SegmentsPruned
            | MetricKind::SegmentsMetadataAnswered
            | MetricKind::SegmentsRead => MetricUnit::Segments,
            MetricKind::ObjectStoreRequests | MetricKind::ObjectStoreRetries => {
                MetricUnit::Requests
            }
            MetricKind::EffectCallsPlanned | MetricKind::EffectCallsExecuted => MetricUnit::Calls,
            _ => MetricUnit::Count,
        };
        Self {
            kind,
            unit,
            value,
            fields: Vec::new(),
        }
    }
    #[must_use]
    pub fn with_unit(mut self, unit: MetricUnit) -> Self {
        self.unit = unit;
        self
    }
    pub fn add_field(&mut self, field: ObservedField) {
        self.fields.push(field);
    }
    #[must_use]
    pub fn has_unsafe_fields(&self) -> bool {
        self.fields.iter().any(|f| !f.is_safe_to_emit())
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}={} {}",
            self.kind.as_str(),
            self.value.to_human_text(),
            self.unit.as_str()
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceSpanId(String);
impl TraceSpanId {
    /// Creates a trace span identifier.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the id is empty.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("trace span id", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceSpanCategory {
    Query,
    Planning,
    Optimization,
    Scan,
    SegmentPruning,
    EncodedEvaluation,
    PartialDecode,
    Materialization,
    Aggregation,
    Join,
    Sort,
    Spill,
    Translation,
    OutputWrite,
    Commit,
    ExternalEffect,
    TaskExecution,
    ObjectStoreRequest,
    Unsupported,
}
impl TraceSpanCategory {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Planning => "planning",
            Self::Optimization => "optimization",
            Self::Scan => "scan",
            Self::SegmentPruning => "segment_pruning",
            Self::EncodedEvaluation => "encoded_evaluation",
            Self::PartialDecode => "partial_decode",
            Self::Materialization => "materialization",
            Self::Aggregation => "aggregation",
            Self::Join => "join",
            Self::Sort => "sort",
            Self::Spill => "spill",
            Self::Translation => "translation",
            Self::OutputWrite => "output_write",
            Self::Commit => "commit",
            Self::ExternalEffect => "external_effect",
            Self::TaskExecution => "task_execution",
            Self::ObjectStoreRequest => "object_store_request",
            Self::Unsupported => "unsupported",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceSpanStatus {
    Planned,
    Running,
    Completed,
    Failed,
    Unsupported,
    NotImplemented,
}
impl TraceSpanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::Failed | Self::Unsupported | Self::NotImplemented
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TraceSpanSkeleton {
    pub id: TraceSpanId,
    pub parent_id: Option<TraceSpanId>,
    pub category: TraceSpanCategory,
    pub name: String,
    pub status: TraceSpanStatus,
    pub fields: Vec<ObservedField>,
    pub metrics: Vec<MetricSample>,
    pub diagnostics: Vec<Diagnostic>,
}
impl TraceSpanSkeleton {
    /// Creates a trace span skeleton record without running tracing collection.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty.
    pub fn new(
        id: TraceSpanId,
        category: TraceSpanCategory,
        name: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        validate_non_empty("trace span name", &name)?;
        Ok(Self {
            id,
            parent_id: None,
            category,
            name,
            status: TraceSpanStatus::Planned,
            fields: Vec::new(),
            metrics: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn with_parent(mut self, parent_id: TraceSpanId) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
    #[must_use]
    pub fn with_status(mut self, status: TraceSpanStatus) -> Self {
        self.status = status;
        self
    }
    pub fn add_field(&mut self, field: ObservedField) {
        self.fields.push(field);
    }
    pub fn add_metric(&mut self, metric: MetricSample) {
        self.metrics.push(metric);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn has_unsafe_fields(&self) -> bool {
        self.fields.iter().any(|f| !f.is_safe_to_emit())
            || self.metrics.iter().any(MetricSample::has_unsafe_fields)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} [{}] ({})",
            self.name,
            self.category.as_str(),
            self.status.as_str()
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredEventKind {
    PlanCreated,
    SegmentPruned,
    MetadataOnlyAnswerUsed,
    EncodedEvaluationUsed,
    PartialDecodeRequired,
    FullMaterializationRequired,
    RuntimeFilterApplied,
    MemoryPressureChanged,
    SpillPlanned,
    SpillCompleted,
    TaskRetried,
    OutputCommitAmbiguous,
    ExternalEffectSkippedDryRun,
    UnsupportedFeatureEncountered,
}
impl StructuredEventKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanCreated => "plan_created",
            Self::SegmentPruned => "segment_pruned",
            Self::MetadataOnlyAnswerUsed => "metadata_only_answer_used",
            Self::EncodedEvaluationUsed => "encoded_evaluation_used",
            Self::PartialDecodeRequired => "partial_decode_required",
            Self::FullMaterializationRequired => "full_materialization_required",
            Self::RuntimeFilterApplied => "runtime_filter_applied",
            Self::MemoryPressureChanged => "memory_pressure_changed",
            Self::SpillPlanned => "spill_planned",
            Self::SpillCompleted => "spill_completed",
            Self::TaskRetried => "task_retried",
            Self::OutputCommitAmbiguous => "output_commit_ambiguous",
            Self::ExternalEffectSkippedDryRun => "external_effect_skipped_dry_run",
            Self::UnsupportedFeatureEncountered => "unsupported_feature_encountered",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StructuredEvent {
    pub kind: StructuredEventKind,
    pub message: String,
    pub fields: Vec<ObservedField>,
    pub diagnostics: Vec<Diagnostic>,
}
impl StructuredEvent {
    /// Creates a structured event skeleton without executing effects.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `message` is empty.
    pub fn new(kind: StructuredEventKind, message: impl Into<String>) -> Result<Self> {
        let message = message.into();
        validate_non_empty("structured event message", &message)?;
        Ok(Self {
            kind,
            message,
            fields: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    pub fn add_field(&mut self, field: ObservedField) {
        self.fields.push(field);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn has_unsafe_fields(&self) -> bool {
        self.fields.iter().any(|f| !f.is_safe_to_emit())
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}: {}", self.kind.as_str(), self.message)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorProfile {
    pub operator_name: String,
    pub operator_kind: String,
    pub metrics: Vec<MetricSample>,
    pub diagnostics: Vec<Diagnostic>,
}
impl OperatorProfile {
    /// Creates an operator profile skeleton.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when name or kind is empty.
    pub fn new(operator_name: impl Into<String>, operator_kind: impl Into<String>) -> Result<Self> {
        let operator_name = operator_name.into();
        let operator_kind = operator_kind.into();
        validate_non_empty("operator name", &operator_name)?;
        validate_non_empty("operator kind", &operator_kind)?;
        Ok(Self {
            operator_name,
            operator_kind,
            metrics: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    pub fn add_metric(&mut self, metric: MetricSample) {
        self.metrics.push(metric);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{} ({})", self.operator_name, self.operator_kind)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct KernelProfile {
    pub kernel_name: String,
    pub dtype: Option<LogicalDType>,
    pub encoding: Option<EncodingKind>,
    pub metrics: Vec<MetricSample>,
    pub diagnostics: Vec<Diagnostic>,
}
impl KernelProfile {
    /// Creates a kernel profile skeleton.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `kernel_name` is empty.
    pub fn new(kernel_name: impl Into<String>) -> Result<Self> {
        let kernel_name = kernel_name.into();
        validate_non_empty("kernel name", &kernel_name)?;
        Ok(Self {
            kernel_name,
            dtype: None,
            encoding: None,
            metrics: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn with_dtype(mut self, dtype: LogicalDType) -> Self {
        self.dtype = Some(dtype);
        self
    }
    #[must_use]
    pub fn with_encoding(mut self, encoding: EncodingKind) -> Self {
        self.encoding = Some(encoding);
        self
    }
    pub fn add_metric(&mut self, metric: MetricSample) {
        self.metrics.push(metric);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} [{} / {}]",
            self.kernel_name,
            self.dtype.as_ref().map_or("unknown", LogicalDType::as_str),
            self.encoding
                .as_ref()
                .map_or("unknown", EncodingKind::as_str)
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilityPlanStatus {
    Planned,
    DiagnosticOnly,
    CollectionNotImplemented,
    Unsupported,
}
impl ObservabilityPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::DiagnosticOnly => "diagnostic_only",
            Self::CollectionNotImplemented => "collection_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::CollectionNotImplemented | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ObservabilityPlan {
    pub surfaces: Vec<ObservabilitySurface>,
    pub metrics: Vec<MetricKind>,
    pub spans: Vec<TraceSpanCategory>,
    pub redaction_required: bool,
    pub status: ObservabilityPlanStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl ObservabilityPlan {
    #[must_use]
    pub fn default_foundation_plan() -> Self {
        Self {
            surfaces: vec![
                ObservabilitySurface::CliText,
                ObservabilitySurface::CliJson,
                ObservabilitySurface::ExplainReport,
                ObservabilitySurface::EstimateReport,
                ObservabilitySurface::DoctorReport,
                ObservabilitySurface::CapabilityReport,
                ObservabilitySurface::BenchmarkReport,
                ObservabilitySurface::RuntimeReport,
            ],
            metrics: vec![
                MetricKind::SegmentsConsidered,
                MetricKind::SegmentsPruned,
                MetricKind::BytesRead,
                MetricKind::BytesDecoded,
                MetricKind::RowsMaterialized,
                MetricKind::MemoryReservedBytes,
                MetricKind::SpillBytes,
                MetricKind::OutputBytes,
                MetricKind::DiagnosticsCount,
            ],
            spans: vec![
                TraceSpanCategory::Planning,
                TraceSpanCategory::Scan,
                TraceSpanCategory::Materialization,
            ],
            redaction_required: true,
            status: ObservabilityPlanStatus::Planned,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn collection_not_implemented(
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: ObservabilityPlanStatus::CollectionNotImplemented,
            diagnostics: vec![
                Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    feature.clone(),
                    format!("Collection not implemented: {reason}"),
                    Some(
                        "Use observability planning/reporting skeleton commands only.".to_string(),
                    ),
                ),
                Diagnostic::no_fallback_execution(
                    "Fallback execution remains disabled for observability collection.",
                ),
            ],
            ..Self::default_foundation_plan()
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: ObservabilityPlanStatus::Unsupported,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Use supported observability planning surfaces only.".to_string()),
            )],
            ..Self::default_foundation_plan()
        }
    }
    pub fn add_surface(&mut self, surface: ObservabilitySurface) {
        self.surfaces.push(surface);
    }
    pub fn add_metric(&mut self, metric: MetricKind) {
        self.metrics.push(metric);
    }
    pub fn add_span(&mut self, span: TraceSpanCategory) {
        self.spans.push(span);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "Observability plan\nstatus: {}\nsurfaces: {}\nmetrics: {}\nspans: {}\nredaction_required: {}\nfallback execution: disabled",
            self.status.as_str(),
            self.surfaces.len(),
            self.metrics.len(),
            self.spans.len(),
            self.redaction_required
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilitySchemaArea {
    Plan,
    Execution,
    VortexIo,
    ObjectStoreIo,
    MemorySpill,
    TranslationOutput,
    Benchmark,
    Certificate,
    UnsupportedDiagnostics,
}
impl ObservabilitySchemaArea {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Execution => "execution",
            Self::VortexIo => "vortex_io",
            Self::ObjectStoreIo => "object_store_io",
            Self::MemorySpill => "memory_spill",
            Self::TranslationOutput => "translation_output",
            Self::Benchmark => "benchmark",
            Self::Certificate => "certificate",
            Self::UnsupportedDiagnostics => "unsupported_diagnostics",
        }
    }

    #[must_use]
    pub const fn required() -> &'static [Self] {
        &[
            Self::Plan,
            Self::Execution,
            Self::VortexIo,
            Self::ObjectStoreIo,
            Self::MemorySpill,
            Self::TranslationOutput,
            Self::Benchmark,
            Self::Certificate,
            Self::UnsupportedDiagnostics,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilitySchemaStatus {
    Missing,
    ReportOnly,
    RuntimeBacked,
    NotApplicable,
}
impl ObservabilitySchemaStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::ReportOnly => "report_only",
            Self::RuntimeBacked => "runtime_backed",
            Self::NotApplicable => "not_applicable",
        }
    }

    #[must_use]
    pub const fn schema_present(&self) -> bool {
        matches!(self, Self::ReportOnly | Self::RuntimeBacked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilitySchemaCoverageEntry {
    pub area: ObservabilitySchemaArea,
    pub trace_span_schema: ObservabilitySchemaStatus,
    pub structured_event_schema: ObservabilitySchemaStatus,
    pub profile_schema: ObservabilitySchemaStatus,
    pub log_schema: ObservabilitySchemaStatus,
    pub certificate_link_required: bool,
    pub redaction_required: bool,
}
impl ObservabilitySchemaCoverageEntry {
    #[must_use]
    pub const fn report_only(area: ObservabilitySchemaArea) -> Self {
        Self {
            area,
            trace_span_schema: ObservabilitySchemaStatus::ReportOnly,
            structured_event_schema: ObservabilitySchemaStatus::ReportOnly,
            profile_schema: ObservabilitySchemaStatus::ReportOnly,
            log_schema: ObservabilitySchemaStatus::ReportOnly,
            certificate_link_required: true,
            redaction_required: true,
        }
    }

    #[must_use]
    pub const fn schema_complete(&self) -> bool {
        self.trace_span_schema.schema_present()
            && self.structured_event_schema.schema_present()
            && self.profile_schema.schema_present()
            && self.log_schema.schema_present()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObservabilitySchemaCoverageReport {
    pub schema_version: &'static str,
    pub entries: Vec<ObservabilitySchemaCoverageEntry>,
    pub local_json_required: bool,
    pub exporter_integration_enabled: bool,
    pub runtime_collection_enabled: bool,
    pub debug_bundle_schema_present: bool,
    pub redaction_required: bool,
    pub certificate_link_required: bool,
    pub fallback_attempted: bool,
}
impl ObservabilitySchemaCoverageReport {
    #[must_use]
    pub fn rfc0018_foundation() -> Self {
        Self {
            schema_version: "shardloom.observability_schema_coverage.v1",
            entries: ObservabilitySchemaArea::required()
                .iter()
                .copied()
                .map(ObservabilitySchemaCoverageEntry::report_only)
                .collect(),
            local_json_required: true,
            exporter_integration_enabled: false,
            runtime_collection_enabled: false,
            debug_bundle_schema_present: true,
            redaction_required: true,
            certificate_link_required: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn area_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn complete_area_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.schema_complete())
            .count()
    }

    #[must_use]
    pub fn missing_area_count(&self) -> usize {
        let incomplete_entry_count = self
            .entries
            .iter()
            .filter(|entry| !entry.schema_complete())
            .count();
        incomplete_entry_count + self.missing_required_area_count()
    }

    #[must_use]
    pub fn schema_coverage_complete(&self) -> bool {
        self.missing_area_count() == 0
    }

    #[must_use]
    pub fn missing_required_area_count(&self) -> usize {
        ObservabilitySchemaArea::required()
            .iter()
            .filter(|area| !self.has_area(**area))
            .count()
    }

    #[must_use]
    pub fn has_area(&self, area: ObservabilitySchemaArea) -> bool {
        self.entries.iter().any(|entry| entry.area == area)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "observability_schema_coverage_report\nschema_version={}\nareas={}\ncomplete_areas={}\nmissing_areas={}\nlocal_json_required={}\nexporter_integration_enabled={}\nruntime_collection_enabled={}\ndebug_bundle_schema_present={}\nredaction_required={}\ncertificate_link_required={}\nfallback_attempted={}",
            self.schema_version,
            self.area_count(),
            self.complete_area_count(),
            self.missing_area_count(),
            self.local_json_required,
            self.exporter_integration_enabled,
            self.runtime_collection_enabled,
            self.debug_bundle_schema_present,
            self.redaction_required,
            self.certificate_link_required,
            self.fallback_attempted
        )
    }
}

#[must_use]
pub fn plan_observability_schema_coverage() -> ObservabilitySchemaCoverageReport {
    ObservabilitySchemaCoverageReport::rfc0018_foundation()
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RuntimeObservabilityReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub status: ObservabilityPlanStatus,
    pub local_benchmark_span_schema_present: bool,
    pub local_benchmark_stage_timing_schema_present: bool,
    pub benchmark_metadata_surface_present: bool,
    pub local_benchmark_spans_measured: bool,
    pub live_profiling_supported: bool,
    pub distributed_runtime_introspection_supported: bool,
    pub profiler_backend_enabled: bool,
    pub trace_backend_enabled: bool,
    pub exporter_integration_enabled: bool,
    pub runtime_collection_enabled: bool,
    pub profile_artifact_generated: bool,
    pub debug_bundle_generated: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub metrics: Vec<MetricSample>,
    pub spans: Vec<TraceSpanSkeleton>,
    pub events: Vec<StructuredEvent>,
    pub operator_profiles: Vec<OperatorProfile>,
    pub kernel_profiles: Vec<KernelProfile>,
    pub diagnostics: Vec<Diagnostic>,
}
impl RuntimeObservabilityReport {
    #[must_use]
    pub fn not_run() -> Self {
        Self {
            schema_version: "shardloom.runtime_observability_report.v1",
            report_id: "gar-0018-a.runtime_introspection.v1",
            gar_id: "GAR-0018-A",
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
            status: ObservabilityPlanStatus::DiagnosticOnly,
            local_benchmark_span_schema_present: true,
            local_benchmark_stage_timing_schema_present: true,
            benchmark_metadata_surface_present: true,
            local_benchmark_spans_measured: false,
            live_profiling_supported: false,
            distributed_runtime_introspection_supported: false,
            profiler_backend_enabled: false,
            trace_backend_enabled: false,
            exporter_integration_enabled: false,
            runtime_collection_enabled: false,
            profile_artifact_generated: false,
            debug_bundle_generated: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            metrics: Vec::new(),
            spans: Vec::new(),
            events: Vec::new(),
            operator_profiles: Vec::new(),
            kernel_profiles: Vec::new(),
            diagnostics: vec![
                Self::report_only_blocker(
                    "live_profiling",
                    "Live profiling collection is not implemented for native ShardLoom runtime.",
                ),
                Self::report_only_blocker(
                    "distributed_runtime_introspection",
                    "Distributed runtime introspection is blocked until coordinator, worker, task, and trace-backend evidence exists.",
                ),
                Self::no_fallback_info(),
            ],
        }
    }
    #[must_use]
    pub fn from_plan(plan: &ObservabilityPlan) -> Self {
        let mut report = Self::not_run();
        report.status = plan.status;
        report.diagnostics.clone_from(&plan.diagnostics);
        report
    }
    pub fn add_metric(&mut self, metric: MetricSample) {
        self.metrics.push(metric);
    }
    pub fn add_span(&mut self, span: TraceSpanSkeleton) {
        self.spans.push(span);
    }
    pub fn add_event(&mut self, event: StructuredEvent) {
        self.events.push(event);
    }
    pub fn add_operator_profile(&mut self, profile: OperatorProfile) {
        self.operator_profiles.push(profile);
    }
    pub fn add_kernel_profile(&mut self, profile: KernelProfile) {
        self.kernel_profiles.push(profile);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn local_benchmark_stage_timing_fields(&self) -> Vec<&'static str> {
        vec![
            "source_read_millis",
            "compatibility_parse_millis",
            "compatibility_to_vortex_import_millis",
            "vortex_write_millis",
            "vortex_reopen_millis",
            "vortex_scan_millis",
            "operator_compute_millis",
            "result_sink_write_millis",
            "evidence_render_millis",
            "total_runtime_millis",
        ]
    }
    #[must_use]
    pub fn local_benchmark_stage_timing_field_count(&self) -> usize {
        self.local_benchmark_stage_timing_fields().len()
    }
    #[must_use]
    pub fn runtime_blocker_order(&self) -> Vec<&'static str> {
        vec![
            "live_profiling_collector",
            "distributed_trace_backend",
            "profiler_backend",
            "metrics_exporter",
            "coordinator_worker_runtime",
            "execution_certificate",
            "native_io_certificate",
            "redaction_policy",
            "no_fallback_evidence",
        ]
    }
    #[must_use]
    pub fn runtime_blocker_count(&self) -> usize {
        self.runtime_blocker_order().len()
    }
    #[must_use]
    pub fn no_runtime_collection_or_external_effects(&self) -> bool {
        !self.local_benchmark_spans_measured
            && !self.live_profiling_supported
            && !self.distributed_runtime_introspection_supported
            && !self.profiler_backend_enabled
            && !self.trace_backend_enabled
            && !self.exporter_integration_enabled
            && !self.runtime_collection_enabled
            && !self.profile_artifact_generated
            && !self.debug_bundle_generated
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.spans.iter().any(TraceSpanSkeleton::has_errors)
            || self.events.iter().any(StructuredEvent::has_errors)
            || self
                .operator_profiles
                .iter()
                .any(OperatorProfile::has_errors)
            || self.kernel_profiles.iter().any(KernelProfile::has_errors)
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn has_unsafe_fields(&self) -> bool {
        self.metrics.iter().any(MetricSample::has_unsafe_fields)
            || self.spans.iter().any(TraceSpanSkeleton::has_unsafe_fields)
            || self.events.iter().any(StructuredEvent::has_unsafe_fields)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "Runtime observability report\nstatus: {}\nsupport_status: {}\nclaim_gate_status: {}\nlocal benchmark stage timing schema: {}\nmetrics: {}\nspans: {}\nevents: {}\noperator_profiles: {}\nkernel_profiles: {}\nlive profiling: unsupported\ndistributed introspection: unsupported\nfallback execution: disabled",
            self.status.as_str(),
            self.support_status,
            self.claim_gate_status,
            self.local_benchmark_stage_timing_schema_present,
            self.metrics.len(),
            self.spans.len(),
            self.events.len(),
            self.operator_profiles.len(),
            self.kernel_profiles.len()
        )
    }
    fn report_only_blocker(feature: &str, message: &str) -> Diagnostic {
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Info,
            DiagnosticCategory::UnsupportedFeature,
            message,
            Some(feature.to_string()),
            Some("Report-only introspection surface; runtime collection is not executed.".to_string()),
            Some("Use benchmark stage timing fields and execution certificates until native profiling is certified.".to_string()),
            FallbackStatus::disabled_by_policy(),
        )
    }
    fn no_fallback_info() -> Diagnostic {
        Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Info,
            DiagnosticCategory::NoFallbackPolicy,
            "Runtime observability reporting does not invoke external engines or fallback execution.",
            None,
            Some("Observability collection is report-only in this slice.".to_string()),
            Some("Keep fallback_attempted=false and external_engine_invoked=false for all profiling blockers.".to_string()),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct OpenLineageFacetMappingRow {
    pub row_id: &'static str,
    pub facet_name: &'static str,
    pub facet_key: &'static str,
    pub openlineage_entity: &'static str,
    pub shardloom_evidence_fields: &'static str,
    pub schema_url_placeholder: &'static str,
    pub schema_version: &'static str,
    pub producer: &'static str,
    pub facet_status: &'static str,
    pub export_enabled: bool,
    pub event_emitted: bool,
    pub network_call_performed: bool,
    pub redaction_required: bool,
    pub retention_policy_required: bool,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl OpenLineageFacetMappingRow {
    const PRODUCER: &'static str = "https://github.com/depsilon/shardloom";
    const STATUS: &'static str = "report_only_schema_placeholder";
    const SCHEMA_VERSION: &'static str = "v1";

    #[must_use]
    pub const fn execution_mode() -> Self {
        Self {
            row_id: "execution_mode",
            facet_name: "ExecutionModeFacet",
            facet_key: "shardloom_execution_mode",
            openlineage_entity: "run",
            shardloom_evidence_fields: "execution_mode,engine_mode,provider_kind,selected_mode_reason",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/execution-mode-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps execution-mode evidence to a future run facet only; it does not emit lineage events, hide auto-mode decisions, or prove production lineage support.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn no_fallback() -> Self {
        Self {
            row_id: "no_fallback",
            facet_name: "NoFallbackFacet",
            facet_key: "shardloom_no_fallback",
            openlineage_entity: "run",
            shardloom_evidence_fields: "fallback_attempted,fallback_execution_allowed,external_engine_invoked",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/no-fallback-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Records no-fallback and no-external-engine posture only; it cannot authorize fallback execution or external engine invocation.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn native_io_certificate() -> Self {
        Self {
            row_id: "native_io_certificate",
            facet_name: "NativeIoCertificateFacet",
            facet_key: "shardloom_native_io_certificate",
            openlineage_entity: "input_dataset,output_dataset,run_refs",
            shardloom_evidence_fields: "native_io_certificate_status,native_io_certificate_ref,source_io_performed,output_io_performed,representation_transition",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/native-io-certificate-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps Native I/O certificate refs to future dataset/run facets only; it does not certify new sources, sinks, object stores, or table runtimes.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn materialization_boundary() -> Self {
        Self {
            row_id: "materialization_boundary",
            facet_name: "MaterializationBoundaryFacet",
            facet_key: "shardloom_materialization_boundary",
            openlineage_entity: "run,input_dataset,output_dataset",
            shardloom_evidence_fields: "data_decoded,data_materialized,stayed_encoded,materialization_boundary,representation_state",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/materialization-boundary-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps materialization/decode boundaries only; it does not imply zero-decode or encoded-native execution unless separate evidence supports that claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn claim_gate() -> Self {
        Self {
            row_id: "claim_gate",
            facet_name: "ClaimGateFacet",
            facet_key: "shardloom_claim_gate",
            openlineage_entity: "run",
            shardloom_evidence_fields: "claim_gate_status,claim_boundary,claim_blockers,workload_constitution_refs",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/claim-gate-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps claim gate status only; lineage metadata cannot upgrade runtime, production, performance, Spark-replacement, Foundry, or package claims.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn generated_source() -> Self {
        Self {
            row_id: "generated_source",
            facet_name: "GeneratedSourceFacet",
            facet_key: "shardloom_generated_source",
            openlineage_entity: "run,output_dataset",
            shardloom_evidence_fields: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generated_source_seed,generation_deterministic,generated_source_certificate_status",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/generated-source-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps GeneratedSourceCertificate evidence only; it is not emitted for no-dataset smoke and does not create broad SQL/DataFrame or Foundry generated-output support.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn vortex_artifact() -> Self {
        Self {
            row_id: "vortex_artifact",
            facet_name: "VortexArtifactFacet",
            facet_key: "shardloom_vortex_artifact",
            openlineage_entity: "input_dataset,output_dataset",
            shardloom_evidence_fields: "vortex_artifact_ref,vortex_artifact_digest,layout_summary,encoding_summary,statistics_summary,prepared_state_digest",
            schema_url_placeholder: "https://shardloom.io/schemas/openlineage/vortex-artifact-facet-v1.json",
            schema_version: Self::SCHEMA_VERSION,
            producer: Self::PRODUCER,
            facet_status: Self::STATUS,
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps ShardLoom Vortex artifact evidence only; it does not imply official Vortex endorsement, object-store runtime, or lakehouse/table support.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn report_only(&self) -> bool {
        self.facet_status == Self::STATUS
            && !self.export_enabled
            && !self.event_emitted
            && !self.network_call_performed
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct OpenLineageFacetMappingReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub docs_ref: &'static str,
    pub openlineage_object_model_ref: &'static str,
    pub openlineage_facets_ref: &'static str,
    pub openlineage_custom_facets_ref: &'static str,
    pub producer_placeholder: &'static str,
    pub schema_url_base_placeholder: &'static str,
    pub rows: Vec<OpenLineageFacetMappingRow>,
    pub export_enabled: bool,
    pub event_emitted: bool,
    pub network_call_performed: bool,
    pub backend_configured: bool,
    pub client_dependency_added: bool,
    pub schema_published: bool,
    pub redaction_policy_required: bool,
    pub retention_policy_required: bool,
    pub opt_in_required: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl OpenLineageFacetMappingReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.openlineage_facet_mapping.v1",
            report_id: "gar-novel-1b.openlineage_facet_mapping",
            gar_id: "GAR-NOVEL-1B",
            docs_ref: "docs/architecture/evidence-native-generated-execution-observability-confidence.md",
            openlineage_object_model_ref: "https://openlineage.io/docs/spec/object-model/",
            openlineage_facets_ref: "https://openlineage.io/docs/spec/facets/",
            openlineage_custom_facets_ref: "https://openlineage.io/docs/spec/facets/custom-facets/",
            producer_placeholder: OpenLineageFacetMappingRow::PRODUCER,
            schema_url_base_placeholder: "https://shardloom.io/schemas/openlineage/",
            rows: vec![
                OpenLineageFacetMappingRow::execution_mode(),
                OpenLineageFacetMappingRow::no_fallback(),
                OpenLineageFacetMappingRow::native_io_certificate(),
                OpenLineageFacetMappingRow::materialization_boundary(),
                OpenLineageFacetMappingRow::claim_gate(),
                OpenLineageFacetMappingRow::generated_source(),
                OpenLineageFacetMappingRow::vortex_artifact(),
            ],
            export_enabled: false,
            event_emitted: false,
            network_call_performed: false,
            backend_configured: false,
            client_dependency_added: false,
            schema_published: false,
            redaction_policy_required: true,
            retention_policy_required: true,
            opt_in_required: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn all_rows_report_only(&self) -> bool {
        self.rows
            .iter()
            .all(OpenLineageFacetMappingRow::report_only)
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(OpenLineageFacetMappingRow::fallback_free)
    }

    #[must_use]
    pub fn no_export_side_effects(&self) -> bool {
        !self.export_enabled
            && !self.event_emitted
            && !self.network_call_performed
            && !self.backend_configured
            && !self.client_dependency_added
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct OpenTelemetryTraceExportSpanRow {
    pub row_id: &'static str,
    pub span_name: &'static str,
    pub span_kind: &'static str,
    pub timing_fields: &'static str,
    pub shardloom_attribute_allowlist: &'static str,
    pub redaction_policy: &'static str,
    pub sensitive_fields: &'static str,
    pub metric_refs: &'static str,
    pub span_status: &'static str,
    pub export_enabled: bool,
    pub span_emitted: bool,
    pub metric_emitted: bool,
    pub log_emitted: bool,
    pub network_exporter_enabled: bool,
    pub redaction_required: bool,
    pub retention_policy_required: bool,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl OpenTelemetryTraceExportSpanRow {
    const SPAN_KIND: &'static str = "internal";
    const STATUS: &'static str = "report_only_not_emitted";
    const REDACTION_POLICY: &'static str =
        "allowlist_only_redact_paths_query_text_credentials_headers";

    #[must_use]
    pub const fn request_admission() -> Self {
        Self {
            row_id: "request_admission",
            span_name: "shardloom.request_admission",
            span_kind: Self::SPAN_KIND,
            timing_fields: "request_admission_millis,total_runtime_millis",
            shardloom_attribute_allowlist: "execution_mode,engine_mode,capability_admission_status,selected_mode_reason,claim_gate_status,fallback_attempted,external_engine_invoked",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "query_text,source_location,output_location,credential,headers",
            metric_refs: "request_count,request_admission_millis",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps request admission timing and no-fallback posture to a future internal span only; it does not emit traces or admit runtime support.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn source_read() -> Self {
        Self {
            row_id: "source_read",
            span_name: "shardloom.source_read",
            span_kind: Self::SPAN_KIND,
            timing_fields: "source_read_millis,source_discovery_millis,schema_inference_millis,source_parse_millis",
            shardloom_attribute_allowlist: "source_format,source_io_performed,source_state_digest,row_count_estimate,file_count,byte_size",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "source_location,object_uri,credential,headers",
            metric_refs: "source_read_millis,input_bytes,rows_read",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps local/source-read evidence only; it does not enable object-store reads, credential resolution, or external source probes.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn compatibility_parse() -> Self {
        Self {
            row_id: "compatibility_parse",
            span_name: "shardloom.compatibility_parse",
            span_kind: Self::SPAN_KIND,
            timing_fields: "compatibility_parse_millis,compatibility_to_vortex_import_millis",
            shardloom_attribute_allowlist: "source_format,compatibility_parse_status,generated_source_created,source_io_performed,source_state_digest",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "source_location,raw_parse_error,query_text",
            metric_refs: "compatibility_parse_millis,parsed_rows",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps compatibility import/certification cost only; it is not pure query-speed evidence and does not imply fallback execution.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn vortex_import() -> Self {
        Self {
            row_id: "vortex_import",
            span_name: "shardloom.vortex_import",
            span_kind: Self::SPAN_KIND,
            timing_fields: "compatibility_to_vortex_import_millis,vortex_prepare_millis,vortex_write_millis,vortex_reopen_millis",
            shardloom_attribute_allowlist: "prepared_state_digest,vortex_artifact_digest,layout_summary,encoding_summary,statistics_summary",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "vortex_artifact_ref,source_location,output_location",
            metric_refs: "vortex_prepare_millis,vortex_write_millis,vortex_reopen_millis",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps Vortex preparation/write/reopen timing only; it does not publish artifacts, invoke upstream query-engine integrations, or claim object-store/table runtime.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn vortex_scan() -> Self {
        Self {
            row_id: "vortex_scan",
            span_name: "shardloom.vortex_scan",
            span_kind: Self::SPAN_KIND,
            timing_fields: "vortex_scan_millis,source_backed_scan_millis",
            shardloom_attribute_allowlist: "scan_filter_pushed_down,scan_projection_pushed_down,scan_limit_pushed_down,data_decoded,data_materialized,source_backed_scan_used",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "predicate_text,source_location,vortex_artifact_ref",
            metric_refs: "vortex_scan_millis,rows_scanned,columns_read",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps scan pushdown/source-backed scan evidence only; it does not imply encoded-native execution unless separate end-to-end evidence allows it.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn operator_compute() -> Self {
        Self {
            row_id: "operator_compute",
            span_name: "shardloom.operator_compute",
            span_kind: Self::SPAN_KIND,
            timing_fields: "operator_compute_millis",
            shardloom_attribute_allowlist: "operator_execution_class,fused_pipeline_used,rows_scanned,rows_selected,rows_output,encoded_native_claim_allowed",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "expression_text,predicate_text,udf_body",
            metric_refs: "operator_compute_millis,rows_selected,rows_output",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps operator timing and execution class only; it does not enable UDFs, external effects, SQL/DataFrame runtime, or performance claims.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn result_sink() -> Self {
        Self {
            row_id: "result_sink",
            span_name: "shardloom.result_sink",
            span_kind: Self::SPAN_KIND,
            timing_fields: "result_sink_write_millis,output_write_millis,output_replay_millis",
            shardloom_attribute_allowlist: "output_io_performed,output_format,output_native_io_certificate_status,result_replay_verified,output_digest",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "output_location,object_uri,credential,headers",
            metric_refs: "result_sink_write_millis,output_write_millis,output_replay_millis,bytes_written",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps local result-sink/output evidence only; it does not imply object-store write, lakehouse commit, or Foundry output support.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn evidence_render() -> Self {
        Self {
            row_id: "evidence_render",
            span_name: "shardloom.evidence_render",
            span_kind: Self::SPAN_KIND,
            timing_fields: "evidence_render_millis",
            shardloom_attribute_allowlist: "execution_certificate_status,native_io_certificate_status,materialization_boundary,generated_source_certificate_status,claim_gate_status",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "certificate_path,evidence_artifact_path,raw_diagnostic_message",
            metric_refs: "evidence_render_millis,evidence_artifact_count",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps evidence-rendering cost and certificate refs only; it does not publish telemetry or upgrade claim status.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn claim_gate() -> Self {
        Self {
            row_id: "claim_gate",
            span_name: "shardloom.claim_gate",
            span_kind: Self::SPAN_KIND,
            timing_fields: "claim_gate_millis,evidence_render_millis,total_runtime_millis",
            shardloom_attribute_allowlist: "claim_gate_status,claim_boundary,performance_claim_allowed,production_claim_allowed,scale_claim_status",
            redaction_policy: Self::REDACTION_POLICY,
            sensitive_fields: "private_memo_ref,unredacted_claim_notes,customer_identifier",
            metric_refs: "claim_gate_millis,claim_blocker_count",
            span_status: Self::STATUS,
            export_enabled: false,
            span_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_exporter_enabled: false,
            redaction_required: true,
            retention_policy_required: true,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Maps claim-gate decisions only; telemetry cannot create production, performance, Spark-replacement, Foundry, package, or scale claims.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn report_only(&self) -> bool {
        self.span_status == Self::STATUS
            && !self.export_enabled
            && !self.span_emitted
            && !self.metric_emitted
            && !self.log_emitted
            && !self.network_exporter_enabled
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct OpenTelemetryTraceExportContractReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub docs_ref: &'static str,
    pub opentelemetry_traces_ref: &'static str,
    pub opentelemetry_common_ref: &'static str,
    pub otlp_spec_ref: &'static str,
    pub otlp_exporter_ref: &'static str,
    pub schema_url_base_placeholder: &'static str,
    pub rows: Vec<OpenTelemetryTraceExportSpanRow>,
    pub trace_export_enabled: bool,
    pub metric_export_enabled: bool,
    pub log_export_enabled: bool,
    pub otlp_exporter_configured: bool,
    pub network_exporter_enabled: bool,
    pub collector_configured: bool,
    pub sdk_dependency_added: bool,
    pub runtime_collection_enabled: bool,
    pub trace_emitted: bool,
    pub metric_emitted: bool,
    pub log_emitted: bool,
    pub network_call_performed: bool,
    pub attribute_allowlist_required: bool,
    pub redaction_policy_required: bool,
    pub retention_policy_required: bool,
    pub opt_in_required: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl OpenTelemetryTraceExportContractReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.opentelemetry_trace_export_contract.v1",
            report_id: "gar-novel-1c.opentelemetry_trace_export_contract",
            gar_id: "GAR-NOVEL-1C",
            docs_ref: "docs/architecture/evidence-native-generated-execution-observability-confidence.md",
            opentelemetry_traces_ref: "https://opentelemetry.io/docs/concepts/signals/traces/",
            opentelemetry_common_ref: "https://opentelemetry.io/docs/specs/otel/common/",
            otlp_spec_ref: "https://opentelemetry.io/docs/specs/otlp/",
            otlp_exporter_ref: "https://opentelemetry.io/docs/specs/otel/protocol/exporter/",
            schema_url_base_placeholder: "https://shardloom.io/schemas/opentelemetry/",
            rows: vec![
                OpenTelemetryTraceExportSpanRow::request_admission(),
                OpenTelemetryTraceExportSpanRow::source_read(),
                OpenTelemetryTraceExportSpanRow::compatibility_parse(),
                OpenTelemetryTraceExportSpanRow::vortex_import(),
                OpenTelemetryTraceExportSpanRow::vortex_scan(),
                OpenTelemetryTraceExportSpanRow::operator_compute(),
                OpenTelemetryTraceExportSpanRow::result_sink(),
                OpenTelemetryTraceExportSpanRow::evidence_render(),
                OpenTelemetryTraceExportSpanRow::claim_gate(),
            ],
            trace_export_enabled: false,
            metric_export_enabled: false,
            log_export_enabled: false,
            otlp_exporter_configured: false,
            network_exporter_enabled: false,
            collector_configured: false,
            sdk_dependency_added: false,
            runtime_collection_enabled: false,
            trace_emitted: false,
            metric_emitted: false,
            log_emitted: false,
            network_call_performed: false,
            attribute_allowlist_required: true,
            redaction_policy_required: true,
            retention_policy_required: true,
            opt_in_required: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn all_rows_report_only(&self) -> bool {
        self.rows
            .iter()
            .all(OpenTelemetryTraceExportSpanRow::report_only)
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(OpenTelemetryTraceExportSpanRow::fallback_free)
    }

    #[must_use]
    pub const fn no_export_side_effects(&self) -> bool {
        !self.trace_export_enabled
            && !self.metric_export_enabled
            && !self.log_export_enabled
            && !self.otlp_exporter_configured
            && !self.network_exporter_enabled
            && !self.collector_configured
            && !self.sdk_dependency_added
            && !self.runtime_collection_enabled
            && !self.trace_emitted
            && !self.metric_emitted
            && !self.log_emitted
            && !self.network_call_performed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn surface_json_machine_readable() {
        assert!(ObservabilitySurface::CliJson.is_machine_readable());
    }
    #[test]
    fn surface_text_not_machine_readable() {
        assert!(!ObservabilitySurface::CliText.is_machine_readable());
    }
    #[test]
    fn secret_requires_redaction() {
        assert!(SensitivityLevel::Secret.requires_redaction());
    }
    #[test]
    fn redacted_safe() {
        assert!(RedactionStatus::Redacted.is_safe());
    }
    #[test]
    fn required_missing_unsafe() {
        assert!(!RedactionStatus::RequiredButMissing.is_safe());
    }
    #[test]
    fn observed_field_rejects_empty_key() {
        assert!(ObservedField::new(" ", "v").is_err());
    }
    #[test]
    fn secret_not_raw() {
        let f = ObservedField::secret("api_key").unwrap();
        assert_eq!(f.value, "[secret]");
    }
    #[test]
    fn safe_value_behavior() {
        let red = ObservedField::sensitive("k", "v").unwrap().redacted();
        assert_eq!(red.safe_value(), "[redacted]");
        let omit = ObservedField::sensitive("k", "v").unwrap().omitted();
        assert_eq!(omit.safe_value(), "[omitted]");
        let uns = ObservedField::sensitive("k", "v").unwrap();
        assert_eq!(uns.safe_value(), "[unsafe]");
    }
    #[test]
    fn bytes_read_category() {
        assert_eq!(MetricKind::BytesRead.category(), MetricCategory::VortexScan);
    }
    #[test]
    fn metric_sample_infers_bytes() {
        assert_eq!(
            MetricSample::new(MetricKind::BytesRead, ObservabilityMetricValue::U64(1)).unit,
            MetricUnit::Bytes
        );
    }
    #[test]
    fn trace_span_id_rejects_empty() {
        assert!(TraceSpanId::new(" ").is_err());
    }
    #[test]
    fn trace_span_rejects_empty_name() {
        let id = TraceSpanId::new("s").unwrap();
        assert!(TraceSpanSkeleton::new(id, TraceSpanCategory::Query, " ").is_err());
    }
    #[test]
    fn failed_span_status_error() {
        assert!(TraceSpanStatus::Failed.is_error());
    }
    #[test]
    fn event_rejects_empty_message() {
        assert!(StructuredEvent::new(StructuredEventKind::PlanCreated, " ").is_err());
    }
    #[test]
    fn operator_rejects_empty() {
        assert!(OperatorProfile::new(" ", "scan").is_err());
        assert!(OperatorProfile::new("op", " ").is_err());
    }
    #[test]
    fn kernel_rejects_empty() {
        assert!(KernelProfile::new(" ").is_err());
    }
    #[test]
    fn default_plan_has_expected_entries() {
        let p = ObservabilityPlan::default_foundation_plan();
        assert!(!p.surfaces.is_empty());
        assert!(p.metrics.contains(&MetricKind::SegmentsConsidered));
    }
    #[test]
    fn collection_not_implemented_has_errors_and_no_fallback() {
        let p = ObservabilityPlan::collection_not_implemented("profiling", "not yet");
        assert!(p.has_errors());
        assert!(p.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
    #[test]
    fn plan_human_text_mentions_fallback_disabled() {
        assert!(
            ObservabilityPlan::default_foundation_plan()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn observability_schema_coverage_covers_required_areas_without_runtime_collection() {
        let report = plan_observability_schema_coverage();

        assert_eq!(
            report.area_count(),
            ObservabilitySchemaArea::required().len()
        );
        assert!(report.schema_coverage_complete());
        assert!(report.has_area(ObservabilitySchemaArea::VortexIo));
        assert!(report.has_area(ObservabilitySchemaArea::ObjectStoreIo));
        assert!(report.has_area(ObservabilitySchemaArea::Certificate));
        assert!(report.has_area(ObservabilitySchemaArea::UnsupportedDiagnostics));
        assert!(!report.exporter_integration_enabled);
        assert!(!report.runtime_collection_enabled);
        assert!(!report.fallback_attempted);
    }
    #[test]
    fn observability_schema_entries_require_redaction_and_certificate_links() {
        let report = ObservabilitySchemaCoverageReport::rfc0018_foundation();

        assert!(report.local_json_required);
        assert!(report.debug_bundle_schema_present);
        assert!(report.redaction_required);
        assert!(report.certificate_link_required);
        assert!(
            report
                .entries
                .iter()
                .all(ObservabilitySchemaCoverageEntry::schema_complete)
        );
        assert!(
            report
                .entries
                .iter()
                .all(|entry| entry.redaction_required && entry.certificate_link_required)
        );
        assert!(
            report
                .to_human_text()
                .contains("runtime_collection_enabled=false")
        );
    }
    #[test]
    fn observability_schema_coverage_requires_each_mandatory_area() {
        let mut report = ObservabilitySchemaCoverageReport::rfc0018_foundation();
        report
            .entries
            .retain(|entry| entry.area != ObservabilitySchemaArea::Certificate);
        report
            .entries
            .push(ObservabilitySchemaCoverageEntry::report_only(
                ObservabilitySchemaArea::Plan,
            ));

        assert_eq!(
            report.area_count(),
            ObservabilitySchemaArea::required().len()
        );
        assert_eq!(report.missing_required_area_count(), 1);
        assert_eq!(report.missing_area_count(), 1);
        assert!(!report.schema_coverage_complete());
    }
    #[test]
    fn runtime_report_from_plan_does_not_collect_metrics() {
        let p = ObservabilityPlan::default_foundation_plan();
        let r = RuntimeObservabilityReport::from_plan(&p);
        assert!(r.metrics.is_empty());
    }
    #[test]
    fn runtime_report_exposes_report_only_introspection_boundaries() {
        let report = RuntimeObservabilityReport::not_run();

        assert_eq!(report.gar_id, "GAR-0018-A");
        assert_eq!(report.support_status, "report_only");
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(report.local_benchmark_span_schema_present);
        assert!(report.local_benchmark_stage_timing_schema_present);
        assert_eq!(report.local_benchmark_stage_timing_field_count(), 10);
        assert_eq!(report.runtime_blocker_count(), 9);
        assert!(report.no_runtime_collection_or_external_effects());
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }
    #[test]
    fn runtime_report_detects_unsafe_fields() {
        let mut r = RuntimeObservabilityReport::not_run();
        let mut m = MetricSample::new(
            MetricKind::DiagnosticsCount,
            ObservabilityMetricValue::U64(1),
        );
        m.add_field(ObservedField::sensitive("token", "abc").unwrap());
        r.add_metric(m);
        assert!(r.has_unsafe_fields());
    }
    #[test]
    fn openlineage_facet_mapping_is_report_only_no_export() {
        let report = OpenLineageFacetMappingReport::report_only();

        assert_eq!(
            report.schema_version,
            "shardloom.openlineage_facet_mapping.v1"
        );
        assert_eq!(
            report.row_order(),
            vec![
                "execution_mode",
                "no_fallback",
                "native_io_certificate",
                "materialization_boundary",
                "claim_gate",
                "generated_source",
                "vortex_artifact",
            ]
        );
        assert!(report.all_rows_report_only());
        assert!(report.all_rows_fallback_free());
        assert!(report.no_export_side_effects());
        assert!(report.redaction_policy_required);
        assert!(report.retention_policy_required);
        assert!(report.opt_in_required);
        assert!(!report.schema_published);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
    }

    #[test]
    fn opentelemetry_trace_export_contract_is_report_only_no_export() {
        let report = OpenTelemetryTraceExportContractReport::report_only();

        assert_eq!(
            report.schema_version,
            "shardloom.opentelemetry_trace_export_contract.v1"
        );
        assert_eq!(
            report.row_order(),
            vec![
                "request_admission",
                "source_read",
                "compatibility_parse",
                "vortex_import",
                "vortex_scan",
                "operator_compute",
                "result_sink",
                "evidence_render",
                "claim_gate",
            ]
        );
        assert!(report.all_rows_report_only());
        assert!(report.all_rows_fallback_free());
        assert!(report.no_export_side_effects());
        assert!(report.attribute_allowlist_required);
        assert!(report.redaction_policy_required);
        assert!(report.retention_policy_required);
        assert!(report.opt_in_required);
        assert!(!report.otlp_exporter_configured);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(
            report
                .rows
                .iter()
                .all(|row| row.redaction_policy.contains("allowlist_only"))
        );
    }
}
