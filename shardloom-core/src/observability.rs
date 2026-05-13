//! Observability, tracing, profiling, and runtime introspection domain skeleton.
//!
//! This module defines planning/reporting types only. It does not collect metrics,
//! emit traces, execute profiling, or perform runtime effects.

use crate::{Diagnostic, DiagnosticCode, EncodingKind, LogicalDType, Result, ShardLoomError};

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
pub struct RuntimeObservabilityReport {
    pub status: ObservabilityPlanStatus,
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
            status: ObservabilityPlanStatus::DiagnosticOnly,
            metrics: Vec::new(),
            spans: Vec::new(),
            events: Vec::new(),
            operator_profiles: Vec::new(),
            kernel_profiles: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn from_plan(plan: &ObservabilityPlan) -> Self {
        Self {
            status: plan.status,
            metrics: Vec::new(),
            spans: Vec::new(),
            events: Vec::new(),
            operator_profiles: Vec::new(),
            kernel_profiles: Vec::new(),
            diagnostics: plan.diagnostics.clone(),
        }
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
            "Runtime observability report\nstatus: {}\nmetrics: {}\nspans: {}\nevents: {}\noperator_profiles: {}\nkernel_profiles: {}\nfallback execution: disabled",
            self.status.as_str(),
            self.metrics.len(),
            self.spans.len(),
            self.events.len(),
            self.operator_profiles.len(),
            self.kernel_profiles.len()
        )
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
}
