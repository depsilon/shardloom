//! Benchmark metadata and correctness-foundation domain types.
//!
//! These types define benchmark scenarios and comparison metadata only. They do
//! not execute benchmarks and must not be interpreted as performance claims.
//! Baseline engines are comparison targets only and are never fallback
//! execution paths.

use crate::{Diagnostic, Result, ShardLoomError};

/// Benchmark baseline engine metadata.
///
/// Baselines are comparison targets only and are never execution fallbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineEngine {
    ShardLoom,
    Spark,
    DataFusion,
    DuckDb,
    Polars,
    Pandas,
    Dask,
    Velox,
    VortexIntegration,
    Other,
}

impl BaselineEngine {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ShardLoom => "shardloom",
            Self::Spark => "spark",
            Self::DataFusion => "datafusion",
            Self::DuckDb => "duckdb",
            Self::Polars => "polars",
            Self::Pandas => "pandas",
            Self::Dask => "dask",
            Self::Velox => "velox",
            Self::VortexIntegration => "vortex_integration",
            Self::Other => "other",
        }
    }

    /// Returns whether fallback execution is allowed for this baseline.
    ///
    /// Always `false` by policy.
    #[must_use]
    pub const fn is_fallback_allowed(&self) -> bool {
        let _ = self;
        false
    }
}

/// High-level class for Spark-displacement and native workload planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadClass {
    SingleNodeEncodedExecution,
    MassiveObjectStoreScan,
    IncrementalRecomputation,
    LargeJoin,
    AggregationAndGrouping,
    TraditionalAnalytics,
    NativeOutputAndTranslation,
    FailureAndUnsupportedBehavior,
}

impl WorkloadClass {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SingleNodeEncodedExecution => "single_node_encoded_execution",
            Self::MassiveObjectStoreScan => "massive_object_store_scan",
            Self::IncrementalRecomputation => "incremental_recomputation",
            Self::LargeJoin => "large_join",
            Self::AggregationAndGrouping => "aggregation_and_grouping",
            Self::TraditionalAnalytics => "traditional_analytics",
            Self::NativeOutputAndTranslation => "native_output_and_translation",
            Self::FailureAndUnsupportedBehavior => "failure_and_unsupported_behavior",
        }
    }
}

/// Correctness validation requirements for benchmark scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectnessValidationMode {
    ExpectedOutput,
    DecodedReference,
    DifferentialComparison,
    PropertyBased,
    Fuzz,
    GoldenDiagnostic,
    UnsupportedDiagnosticOnly,
    NotYetDefined,
}

impl CorrectnessValidationMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExpectedOutput => "expected_output",
            Self::DecodedReference => "decoded_reference",
            Self::DifferentialComparison => "differential_comparison",
            Self::PropertyBased => "property_based",
            Self::Fuzz => "fuzz",
            Self::GoldenDiagnostic => "golden_diagnostic",
            Self::UnsupportedDiagnosticOnly => "unsupported_diagnostic_only",
            Self::NotYetDefined => "not_yet_defined",
        }
    }
}

/// Metrics to collect for benchmark and correctness-comparison reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkMetric {
    WallTimeMillis,
    StartupLatencyMillis,
    QueryRuntimeMillis,
    WriteCommitLatencyMillis,
    CpuTimeMillis,
    PeakMemoryBytes,
    Allocations,
    BytesRead,
    BytesDecoded,
    BytesDecodeAvoided,
    BytesWritten,
    RowsScanned,
    RowsMaterialized,
    RowsMaterializationAvoided,
    SegmentsConsidered,
    SegmentsPruned,
    SegmentsMetadataAnswered,
    WorkAvoidedUnits,
    SpillRequiredBytes,
    SpillAvoidedBytes,
    ObjectStoreRequests,
    OutputFiles,
    OutputBytes,
    CostProxy,
}

impl BenchmarkMetric {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::WallTimeMillis => "wall_time_millis",
            Self::StartupLatencyMillis => "startup_latency_millis",
            Self::QueryRuntimeMillis => "query_runtime_millis",
            Self::WriteCommitLatencyMillis => "write_commit_latency_millis",
            Self::CpuTimeMillis => "cpu_time_millis",
            Self::PeakMemoryBytes => "peak_memory_bytes",
            Self::Allocations => "allocations",
            Self::BytesRead => "bytes_read",
            Self::BytesDecoded => "bytes_decoded",
            Self::BytesDecodeAvoided => "bytes_decode_avoided",
            Self::BytesWritten => "bytes_written",
            Self::RowsScanned => "rows_scanned",
            Self::RowsMaterialized => "rows_materialized",
            Self::RowsMaterializationAvoided => "rows_materialization_avoided",
            Self::SegmentsConsidered => "segments_considered",
            Self::SegmentsPruned => "segments_pruned",
            Self::SegmentsMetadataAnswered => "segments_metadata_answered",
            Self::WorkAvoidedUnits => "work_avoided_units",
            Self::SpillRequiredBytes => "spill_required_bytes",
            Self::SpillAvoidedBytes => "spill_avoided_bytes",
            Self::ObjectStoreRequests => "object_store_requests",
            Self::OutputFiles => "output_files",
            Self::OutputBytes => "output_bytes",
            Self::CostProxy => "cost_proxy",
        }
    }
}

/// Scalar metric value or unknown placeholder for not-yet-implemented collection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricValue {
    U64(u64),
    F64(f64),
    Unknown,
}

impl MetricValue {
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        match self {
            Self::U64(value) => value.to_string(),
            Self::F64(value) => format!("{value:.4}"),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

fn non_empty_option(value: Option<&str>) -> bool {
    value.is_some_and(|text| !text.trim().is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkClaimStatus {
    EvidenceMissing,
    ReadyToPublish,
}

impl BenchmarkClaimStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvidenceMissing => "evidence_missing",
            Self::ReadyToPublish => "ready_to_publish",
        }
    }

    #[must_use]
    pub const fn can_publish(&self) -> bool {
        matches!(self, Self::ReadyToPublish)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkEvidenceState {
    Missing,
    Present,
}

impl BenchmarkEvidenceState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Present => "present",
        }
    }

    #[must_use]
    pub const fn is_present(&self) -> bool {
        matches!(self, Self::Present)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkFallbackState {
    NotAttempted,
    Attempted,
}

impl BenchmarkFallbackState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::Attempted => "attempted",
        }
    }

    #[must_use]
    pub const fn attempted(&self) -> bool {
        matches!(self, Self::Attempted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkComparisonStatus {
    EvidenceMissing,
    ReadyForComparisonReview,
}

impl BenchmarkComparisonStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvidenceMissing => "evidence_missing",
            Self::ReadyForComparisonReview => "ready_for_comparison_review",
        }
    }

    #[must_use]
    pub const fn is_ready_for_comparison_review(&self) -> bool {
        matches!(self, Self::ReadyForComparisonReview)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkReproducibilityStatus {
    Incomplete,
    Reproducible,
}

impl BenchmarkReproducibilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Incomplete => "incomplete",
            Self::Reproducible => "reproducible",
        }
    }

    #[must_use]
    pub const fn is_reproducible(&self) -> bool {
        matches!(self, Self::Reproducible)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkCacheState {
    Unknown,
    Cold,
    Warm,
    Mixed,
}

impl BenchmarkCacheState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Mixed => "mixed",
        }
    }

    #[must_use]
    pub const fn is_declared(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkDatasetProfile {
    pub scenario_name: String,
    pub dataset_name: Option<String>,
    pub dataset_scale: Option<String>,
    pub schema_profile: Option<String>,
    pub storage_format: Option<String>,
    pub compression: Option<String>,
}

impl BenchmarkDatasetProfile {
    #[must_use]
    pub fn from_scenario(scenario: &BenchmarkScenario) -> Self {
        Self {
            scenario_name: scenario.name.clone(),
            dataset_name: scenario.dataset_name.clone(),
            dataset_scale: scenario.dataset_scale.clone(),
            schema_profile: None,
            storage_format: scenario.storage_format.clone(),
            compression: None,
        }
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        non_empty_option(self.dataset_name.as_deref())
            && non_empty_option(self.dataset_scale.as_deref())
            && non_empty_option(self.schema_profile.as_deref())
            && non_empty_option(self.storage_format.as_deref())
            && non_empty_option(self.compression.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkEngineVersion {
    pub engine: BaselineEngine,
    pub version: String,
}

impl BenchmarkEngineVersion {
    /// Creates a comparison engine-version label.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the version is empty.
    pub fn new(engine: BaselineEngine, version: impl Into<String>) -> Result<Self> {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "benchmark engine version must not be empty".to_string(),
            ));
        }
        Ok(Self { engine, version })
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkResultGap {
    pub scenario_name: String,
    pub engine: BaselineEngine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkMetricGap {
    pub scenario_name: String,
    pub engine: BaselineEngine,
    pub metric: BenchmarkMetric,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkRunManifest {
    pub manifest_id: String,
    pub status: BenchmarkReproducibilityStatus,
    pub scenario_count: usize,
    pub dataset_profiles: Vec<BenchmarkDatasetProfile>,
    pub engine_versions: Vec<BenchmarkEngineVersion>,
    pub missing_engine_versions: Vec<BaselineEngine>,
    pub hardware_profile: Option<String>,
    pub operating_system_profile: Option<String>,
    pub runtime_configuration: Option<String>,
    pub cache_state: BenchmarkCacheState,
    pub required_metrics: Vec<BenchmarkMetric>,
    pub reproduction_steps: Vec<String>,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub fallback: BenchmarkFallbackState,
    pub diagnostics: Vec<Diagnostic>,
}

impl BenchmarkRunManifest {
    /// Creates an empty benchmark reproducibility manifest.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `manifest_id` is empty.
    pub fn new(manifest_id: impl Into<String>) -> Result<Self> {
        let manifest_id = manifest_id.into();
        if manifest_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "benchmark run manifest id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            manifest_id,
            status: BenchmarkReproducibilityStatus::Incomplete,
            scenario_count: 0,
            dataset_profiles: Vec::new(),
            engine_versions: Vec::new(),
            missing_engine_versions: Vec::new(),
            hardware_profile: None,
            operating_system_profile: None,
            runtime_configuration: None,
            cache_state: BenchmarkCacheState::Unknown,
            required_metrics: Vec::new(),
            reproduction_steps: Vec::new(),
            correctness_evidence: BenchmarkEvidenceState::Missing,
            fallback: BenchmarkFallbackState::NotAttempted,
            diagnostics: Vec::new(),
        })
    }

    #[must_use]
    pub fn from_plan(plan: &BenchmarkPlan) -> Self {
        let mut manifest = Self {
            manifest_id: "cg6-foundation-benchmark-reproducibility".to_string(),
            status: BenchmarkReproducibilityStatus::Incomplete,
            scenario_count: 0,
            dataset_profiles: Vec::new(),
            engine_versions: Vec::new(),
            missing_engine_versions: Vec::new(),
            hardware_profile: None,
            operating_system_profile: None,
            runtime_configuration: None,
            cache_state: BenchmarkCacheState::Unknown,
            required_metrics: Vec::new(),
            reproduction_steps: Vec::new(),
            correctness_evidence: BenchmarkEvidenceState::Missing,
            fallback: BenchmarkFallbackState::NotAttempted,
            diagnostics: Vec::new(),
        };
        manifest.dataset_profiles = plan
            .scenarios
            .iter()
            .map(BenchmarkDatasetProfile::from_scenario)
            .collect();
        manifest.refresh_against_plan(plan);
        manifest
    }

    pub fn add_engine_version(&mut self, version: BenchmarkEngineVersion) {
        if let Some(existing) = self
            .engine_versions
            .iter_mut()
            .find(|existing| existing.engine == version.engine)
        {
            *existing = version;
        } else {
            self.engine_versions.push(version);
        }
    }

    pub fn add_reproduction_step(&mut self, step: impl Into<String>) {
        let step = step.into();
        if !step.trim().is_empty() {
            self.reproduction_steps.push(step);
        }
    }

    #[must_use]
    pub fn has_engine_version(&self, engine: BaselineEngine) -> bool {
        self.engine_versions
            .iter()
            .any(|version| version.engine == engine && !version.version.trim().is_empty())
    }

    pub fn refresh_against_plan(&mut self, plan: &BenchmarkPlan) {
        self.scenario_count = plan.scenarios.len();
        self.required_metrics = plan.required_metrics();
        self.missing_engine_versions = plan
            .baseline_engines()
            .into_iter()
            .filter(|engine| !self.has_engine_version(*engine))
            .collect();
        self.diagnostics.clear();
        self.status = if self.required_metadata_present(plan) {
            BenchmarkReproducibilityStatus::Reproducible
        } else {
            self.diagnostics.push(Diagnostic::not_implemented(
                "benchmark reproducibility evidence",
                "Benchmark run metadata is incomplete for dataset shape, engine versions, hardware, operating system, runtime configuration, cache state, reproduction steps, correctness evidence, or no-fallback evidence.",
                "Record complete benchmark run metadata before accepting benchmark evidence for performance or superiority claims.",
            ));
            BenchmarkReproducibilityStatus::Incomplete
        };
    }

    #[must_use]
    pub fn required_metadata_present(&self, plan: &BenchmarkPlan) -> bool {
        self.scenario_count == plan.scenarios.len()
            && self.scenario_count > 0
            && !self.required_metrics.is_empty()
            && self.missing_engine_versions.is_empty()
            && plan.baselines_are_fallback_free()
            && plan.scenarios.iter().all(|scenario| {
                self.dataset_profiles
                    .iter()
                    .any(|profile| profile.scenario_name == scenario.name && profile.is_complete())
            })
            && non_empty_option(self.hardware_profile.as_deref())
            && non_empty_option(self.operating_system_profile.as_deref())
            && non_empty_option(self.runtime_configuration.as_deref())
            && self.cache_state.is_declared()
            && !self.reproduction_steps.is_empty()
            && self.correctness_evidence.is_present()
            && !self.fallback.attempted()
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn evidence_state(&self) -> BenchmarkEvidenceState {
        if self.status.is_reproducible() {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        }
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "benchmark run manifest\nmanifest: {}\nreproducibility status: {}\nscenarios: {}\nrequired metrics: {}\nengine versions: {}\nmissing engine versions: {}\ncache state: {}\nfallback execution: disabled",
            self.manifest_id,
            self.status.as_str(),
            self.scenario_count,
            self.required_metrics.len(),
            self.engine_versions.len(),
            self.missing_engine_versions.len(),
            self.cache_state.as_str(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkClaimGate {
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub required_metrics: BenchmarkEvidenceState,
    pub comparison_report: BenchmarkEvidenceState,
    pub reproducibility_evidence: BenchmarkEvidenceState,
    pub fallback: BenchmarkFallbackState,
    pub status: BenchmarkClaimStatus,
}

impl BenchmarkClaimGate {
    #[must_use]
    pub const fn new(
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        required_metrics: BenchmarkEvidenceState,
        comparison_report: BenchmarkEvidenceState,
        reproducibility_evidence: BenchmarkEvidenceState,
        fallback: BenchmarkFallbackState,
    ) -> Self {
        let status = if correctness_evidence.is_present()
            && benchmark_evidence.is_present()
            && required_metrics.is_present()
            && comparison_report.is_present()
            && reproducibility_evidence.is_present()
            && !fallback.attempted()
        {
            BenchmarkClaimStatus::ReadyToPublish
        } else {
            BenchmarkClaimStatus::EvidenceMissing
        };
        Self {
            correctness_evidence,
            benchmark_evidence,
            required_metrics,
            comparison_report,
            reproducibility_evidence,
            fallback,
            status,
        }
    }

    #[must_use]
    pub const fn can_publish_performance_claim(&self) -> bool {
        self.status.can_publish()
            && self.correctness_evidence.is_present()
            && self.benchmark_evidence.is_present()
            && self.required_metrics.is_present()
            && self.comparison_report.is_present()
            && self.reproducibility_evidence.is_present()
            && !self.fallback.attempted()
    }
}

/// Benchmark scenario metadata used to define reproducible, correctness-first plans.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkScenario {
    pub name: String,
    pub workload_class: WorkloadClass,
    pub dataset_name: Option<String>,
    pub dataset_scale: Option<String>,
    pub storage_format: Option<String>,
    pub query_or_operation: Option<String>,
    pub correctness_validation: CorrectnessValidationMode,
    pub baselines: Vec<BaselineEngine>,
    pub required_metrics: Vec<BenchmarkMetric>,
}

impl BenchmarkScenario {
    /// Constructs a scenario and rejects empty names.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or whitespace-only.
    pub fn new(name: impl Into<String>, workload_class: WorkloadClass) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "benchmark scenario name must not be empty".to_string(),
            ));
        }

        Ok(Self {
            name,
            workload_class,
            dataset_name: None,
            dataset_scale: None,
            storage_format: None,
            query_or_operation: None,
            correctness_validation: CorrectnessValidationMode::NotYetDefined,
            baselines: Vec::new(),
            required_metrics: Vec::new(),
        })
    }

    pub fn add_baseline(&mut self, baseline: BaselineEngine) {
        if !self.baselines.contains(&baseline) {
            self.baselines.push(baseline);
        }
    }

    pub fn add_required_metric(&mut self, metric: BenchmarkMetric) {
        if !self.required_metrics.contains(&metric) {
            self.required_metrics.push(metric);
        }
    }

    #[must_use]
    pub fn requires_metric(&self, metric: BenchmarkMetric) -> bool {
        self.required_metrics.contains(&metric)
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        let _ = self;
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let baseline_text = if self.baselines.is_empty() {
            "none".to_string()
        } else {
            self.baselines
                .iter()
                .map(BaselineEngine::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        };

        format!(
            "scenario: {}\nworkload class: {}\ncorrectness validation: {}\nbaselines (comparison targets only): {}\nfallback execution: disabled",
            self.name,
            self.workload_class.as_str(),
            self.correctness_validation.as_str(),
            baseline_text,
        )
    }
}

/// One engine's metrics and diagnostics for a single benchmark scenario.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkResult {
    pub scenario_name: String,
    pub engine: BaselineEngine,
    pub metrics: Vec<(BenchmarkMetric, MetricValue)>,
    pub diagnostics: Vec<Diagnostic>,
    pub fallback: BenchmarkFallbackState,
}

impl BenchmarkResult {
    /// Creates a benchmark result container for one scenario and engine.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `scenario_name` is empty or whitespace-only.
    pub fn new(scenario_name: impl Into<String>, engine: BaselineEngine) -> Result<Self> {
        let scenario_name = scenario_name.into();
        if scenario_name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "benchmark result scenario name must not be empty".to_string(),
            ));
        }

        Ok(Self {
            scenario_name,
            engine,
            metrics: Vec::new(),
            diagnostics: Vec::new(),
            fallback: BenchmarkFallbackState::NotAttempted,
        })
    }

    pub fn add_metric(&mut self, metric: BenchmarkMetric, value: MetricValue) {
        if let Some(existing) = self
            .metrics
            .iter_mut()
            .find(|(candidate, _)| *candidate == metric)
        {
            *existing = (metric, value);
        } else {
            self.metrics.push((metric, value));
        }
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn metric_value(&self, metric: BenchmarkMetric) -> Option<MetricValue> {
        self.metrics
            .iter()
            .find_map(|(candidate, value)| (*candidate == metric).then_some(*value))
    }

    #[must_use]
    pub fn has_known_metric(&self, metric: BenchmarkMetric) -> bool {
        self.metric_value(metric)
            .is_some_and(|value| value.is_known())
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "benchmark result\nscenario: {}\nengine: {}\nmetrics: {}\ndiagnostics: {}\nfallback execution: disabled",
            self.scenario_name,
            self.engine.as_str(),
            self.metrics.len(),
            self.diagnostics.len(),
        )
    }
}

/// Report-only comparison evidence assembled from benchmark results.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkComparisonReport {
    pub status: BenchmarkComparisonStatus,
    pub scenario_count: usize,
    pub expected_result_count: usize,
    pub results: Vec<BenchmarkResult>,
    pub required_metrics: Vec<BenchmarkMetric>,
    pub missing_results: Vec<BenchmarkResultGap>,
    pub missing_metrics: Vec<BenchmarkMetricGap>,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub fallback: BenchmarkFallbackState,
    pub diagnostics: Vec<Diagnostic>,
}

impl BenchmarkComparisonReport {
    #[must_use]
    pub fn from_plan(plan: &BenchmarkPlan) -> Self {
        Self::from_plan_and_results(plan, Vec::new(), BenchmarkEvidenceState::Missing)
    }

    #[must_use]
    pub fn from_plan_and_results(
        plan: &BenchmarkPlan,
        results: Vec<BenchmarkResult>,
        correctness_evidence: BenchmarkEvidenceState,
    ) -> Self {
        let mut missing_results = Vec::new();
        let mut missing_metrics = Vec::new();

        for scenario in &plan.scenarios {
            for engine in &scenario.baselines {
                if let Some(result) = results.iter().find(|result| {
                    result.scenario_name == scenario.name && result.engine == *engine
                }) {
                    for metric in &scenario.required_metrics {
                        if !result.has_known_metric(*metric) {
                            missing_metrics.push(BenchmarkMetricGap {
                                scenario_name: scenario.name.clone(),
                                engine: *engine,
                                metric: *metric,
                            });
                        }
                    }
                } else {
                    missing_results.push(BenchmarkResultGap {
                        scenario_name: scenario.name.clone(),
                        engine: *engine,
                    });
                }
            }
        }

        let required_metrics = plan.required_metrics();
        let benchmark_evidence = if !results.is_empty()
            && missing_results.is_empty()
            && missing_metrics.is_empty()
            && !required_metrics.is_empty()
        {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        };
        let fallback = if results.iter().any(|result| result.fallback.attempted()) {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        };
        let required_metrics_evidence = if required_metrics.is_empty() {
            BenchmarkEvidenceState::Missing
        } else {
            BenchmarkEvidenceState::Present
        };
        let status = if correctness_evidence.is_present()
            && benchmark_evidence.is_present()
            && required_metrics_evidence.is_present()
            && !fallback.attempted()
        {
            BenchmarkComparisonStatus::ReadyForComparisonReview
        } else {
            BenchmarkComparisonStatus::EvidenceMissing
        };
        let mut diagnostics = Vec::new();
        if !status.is_ready_for_comparison_review() {
            diagnostics.push(Diagnostic::not_implemented(
                "benchmark comparison evidence",
                "Benchmark execution and comparison evidence has not been collected for every required scenario, baseline, and metric.",
                "Run an approved native benchmark harness in a later CG-6 step before publishing performance or superiority claims.",
            ));
        }

        Self {
            status,
            scenario_count: plan.scenarios.len(),
            expected_result_count: plan.expected_result_count(),
            results,
            required_metrics,
            missing_results,
            missing_metrics,
            correctness_evidence,
            benchmark_evidence,
            fallback,
            diagnostics,
        }
    }

    #[must_use]
    pub fn claim_gate(&self) -> BenchmarkClaimGate {
        BenchmarkClaimGate::new(
            self.correctness_evidence,
            self.benchmark_evidence,
            if self.required_metrics.is_empty() {
                BenchmarkEvidenceState::Missing
            } else {
                BenchmarkEvidenceState::Present
            },
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Missing,
            self.fallback,
        )
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "benchmark comparison report\nstatus: {}\ncomparison report emitted: true\nclaim gate: {}\nscenarios: {}\nexpected results: {}\nresults: {}\nmissing results: {}\nmissing metrics: {}\nfallback execution: disabled",
            self.status.as_str(),
            self.claim_gate().status.as_str(),
            self.scenario_count,
            self.expected_result_count,
            self.results.len(),
            self.missing_results.len(),
            self.missing_metrics.len(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkEvidenceBundle {
    pub run_manifest: BenchmarkRunManifest,
    pub comparison_report: BenchmarkComparisonReport,
    pub claim_gate: BenchmarkClaimGate,
    pub diagnostics: Vec<Diagnostic>,
}

impl BenchmarkEvidenceBundle {
    #[must_use]
    pub fn from_reports(
        run_manifest: BenchmarkRunManifest,
        comparison_report: BenchmarkComparisonReport,
    ) -> Self {
        let fallback =
            if run_manifest.fallback.attempted() || comparison_report.fallback.attempted() {
                BenchmarkFallbackState::Attempted
            } else {
                BenchmarkFallbackState::NotAttempted
            };
        let metric_sets_match =
            benchmark_required_metric_sets_match(&run_manifest, &comparison_report);
        let scenario_sets_match =
            benchmark_evidence_scenario_sets_match(&run_manifest, &comparison_report);
        let required_metrics = if run_manifest.required_metrics.is_empty()
            || comparison_report.required_metrics.is_empty()
            || !metric_sets_match
        {
            BenchmarkEvidenceState::Missing
        } else {
            BenchmarkEvidenceState::Present
        };
        let claim_gate = BenchmarkClaimGate::new(
            comparison_report.correctness_evidence,
            comparison_report.benchmark_evidence,
            required_metrics,
            if scenario_sets_match && metric_sets_match {
                BenchmarkEvidenceState::Present
            } else {
                BenchmarkEvidenceState::Missing
            },
            run_manifest.evidence_state(),
            fallback,
        );
        let mut diagnostics = run_manifest.diagnostics.clone();
        diagnostics.extend(comparison_report.diagnostics.clone());
        if !scenario_sets_match || !metric_sets_match {
            diagnostics.push(Diagnostic::not_implemented(
                "benchmark evidence compatibility",
                "Benchmark run manifest and comparison report do not describe the same scenario set and required metric set.",
                "Regenerate the benchmark run manifest and comparison report from the same approved benchmark plan before publishing performance or superiority claims.",
            ));
        }
        if !claim_gate.can_publish_performance_claim() {
            diagnostics.push(Diagnostic::not_implemented(
                "benchmark claim evidence bundle",
                "Benchmark claim evidence is incomplete because correctness, benchmark results, required metrics, comparison reports, reproducibility metadata, and no-fallback evidence are not all present.",
                "Complete the reproducible benchmark run manifest and comparison report before publishing performance or superiority claims.",
            ));
        }

        Self {
            run_manifest,
            comparison_report,
            claim_gate,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn can_publish_performance_claim(&self) -> bool {
        self.claim_gate.can_publish_performance_claim()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "benchmark evidence bundle\nclaim gate: {}\nreproducibility: {}\ncomparison: {}\nfallback execution: disabled",
            self.claim_gate.status.as_str(),
            self.run_manifest.status.as_str(),
            self.comparison_report.status.as_str(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkClaimEvidenceStatus {
    ReadyForClaimReview,
    NeedsEvidence,
    UnsafeFallbackPolicy,
}

impl BenchmarkClaimEvidenceStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReadyForClaimReview => "ready_for_claim_review",
            Self::NeedsEvidence => "needs_evidence",
            Self::UnsafeFallbackPolicy => "unsafe_fallback_policy",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::UnsafeFallbackPolicy)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkClaimEvidenceReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub scope: String,
    pub status: BenchmarkClaimEvidenceStatus,
    pub scenario_count: usize,
    pub scenario_name_order: Vec<String>,
    pub workload_class_order: Vec<String>,
    pub required_metric_count: usize,
    pub required_metric_order: Vec<String>,
    pub required_foundation_metric_count: usize,
    pub covered_required_foundation_metric_count: usize,
    pub missing_required_foundation_metrics: Vec<String>,
    pub baseline_count: usize,
    pub baseline_engine_order: Vec<String>,
    pub external_baseline_count: usize,
    pub external_baseline_engine_order: Vec<String>,
    pub expected_result_count: usize,
    pub result_count: usize,
    pub missing_result_count: usize,
    pub missing_external_result_count: usize,
    pub missing_metric_count: usize,
    pub run_manifest_status: BenchmarkReproducibilityStatus,
    pub run_manifest_emitted: bool,
    pub missing_engine_version_count: usize,
    pub dataset_profile_count: usize,
    pub incomplete_dataset_profile_count: usize,
    pub reproduction_step_count: usize,
    pub cache_state: BenchmarkCacheState,
    pub comparison_report_status: BenchmarkComparisonStatus,
    pub comparison_report_emitted: bool,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub required_metrics_evidence: BenchmarkEvidenceState,
    pub comparison_report_evidence: BenchmarkEvidenceState,
    pub reproducibility_evidence: BenchmarkEvidenceState,
    pub claim_gate_status: BenchmarkClaimStatus,
    pub planned_surface_count: usize,
    pub blocked_surface_count: usize,
    pub blocked_surface_order: Vec<String>,
    pub claim_grade_source_backed_benchmark_closeout_required: bool,
    pub claim_grade_source_backed_benchmark_closeout_allowed: bool,
    pub claim_grade_source_backed_benchmark_closeout_blocker_order: Vec<String>,
    pub measured_benchmark_result_rows_required: bool,
    pub measured_benchmark_result_rows_present: bool,
    pub reproducibility_manifest_population_required: bool,
    pub reproducibility_manifest_populated: bool,
    pub approved_comparison_rows_required: bool,
    pub approved_comparison_rows_present: bool,
    pub benchmark_execution_implemented: bool,
    pub benchmark_execution_performed: bool,
    pub external_engine_execution: bool,
    pub query_execution: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub baselines_fallback_free: bool,
    pub performance_claim_allowed: bool,
    pub superiority_claim_allowed: bool,
    pub best_default_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl BenchmarkClaimEvidenceReport {
    #[must_use]
    pub fn surface_order() -> Vec<&'static str> {
        vec![
            "benchmark_plan",
            "required_metrics",
            "correctness_evidence",
            "benchmark_result_rows",
            "external_comparison_results",
            "comparison_report",
            "reproducibility_manifest",
            "no_fallback_policy",
            "claim_publication_gate",
        ]
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.benchmark_execution_performed
            && !self.external_engine_execution
            && !self.query_execution
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "benchmark_claim_evidence(status={}, scope={}, planned_surfaces={}, blocked_surfaces={}, scenarios={}, required_metrics={}, expected_results={}, missing_results={}, missing_external_results={}, reproducibility={}, claim_gate={}, performance_claim_allowed={}, fallback_execution=disabled)",
            self.status.as_str(),
            self.scope,
            self.planned_surface_count,
            self.blocked_surface_count,
            self.scenario_count,
            self.required_metric_count,
            self.expected_result_count,
            self.missing_result_count,
            self.missing_external_result_count,
            self.run_manifest_status.as_str(),
            self.claim_gate_status.as_str(),
            self.performance_claim_allowed,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkDisplacementBenchmarkEvidenceRow {
    pub row_id: &'static str,
    pub workload_family: &'static str,
    pub workload_ref: &'static str,
    pub shardloom_lane: &'static str,
    pub baseline_oracle_lanes: &'static str,
    pub correctness_ref: &'static str,
    pub timing_ref: &'static str,
    pub environment_ref: &'static str,
    pub execution_mode_ref: &'static str,
    pub policy_ref: &'static str,
    pub claim_gate_status: &'static str,
    pub missing_evidence: &'static str,
    pub external_baseline_only: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SparkDisplacementBenchmarkEvidenceMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub docs_ref: &'static str,
    pub source_refs: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<SparkDisplacementBenchmarkEvidenceRow>,
    pub performance_claim_allowed: bool,
    pub superiority_claim_allowed: bool,
    pub spark_displacement_claim_allowed: bool,
    pub benchmark_rerun_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl SparkDisplacementBenchmarkEvidenceMatrixReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.spark_displacement_benchmark_evidence_matrix.v1",
            report_id: "gar-0009-a.spark_displacement_benchmark_evidence_matrix",
            docs_ref: "docs/architecture/spark-displacement-benchmark-evidence-matrix.md",
            source_refs: "docs/rfcs/0009-benchmark-methodology-spark-displacement.md,docs/rfcs/0025-competitive-engine-track-no-fallback-replacement.md,docs/architecture/benchmark-competitive-claim-evidence.md,docs/architecture/benchmark-suite-catalog.md,benchmarks/traditional_analytics/README.md",
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
            rows: spark_displacement_benchmark_evidence_rows(),
            performance_claim_allowed: false,
            superiority_claim_allowed: false,
            spark_displacement_claim_allowed: false,
            benchmark_rerun_performed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn missing_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.missing_evidence).collect()
    }

    #[must_use]
    pub fn all_rows_not_claim_grade(&self) -> bool {
        self.rows
            .iter()
            .all(|row| row.claim_gate_status == "not_claim_grade")
    }

    #[must_use]
    pub fn all_external_lanes_baseline_only(&self) -> bool {
        self.rows.iter().all(|row| {
            row.external_baseline_only && !row.external_engine_invoked && !row.fallback_attempted
        })
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.performance_claim_allowed
            && !self.superiority_claim_allowed
            && !self.spark_displacement_claim_allowed
            && !self.benchmark_rerun_performed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self.all_rows_not_claim_grade()
            && self.all_external_lanes_baseline_only()
    }
}

#[must_use]
pub fn plan_spark_displacement_benchmark_evidence_matrix()
-> SparkDisplacementBenchmarkEvidenceMatrixReport {
    SparkDisplacementBenchmarkEvidenceMatrixReport::report_only()
}

fn spark_displacement_benchmark_evidence_rows() -> Vec<SparkDisplacementBenchmarkEvidenceRow> {
    vec![
        SparkDisplacementBenchmarkEvidenceRow {
            row_id: "compatibility_import_certified_lane",
            workload_family: "compatibility_import_certified",
            workload_ref: "local CSV/Parquet/JSONL/Arrow/Avro/ORC compatibility import rows",
            shardloom_lane: "compatibility_import_certified",
            baseline_oracle_lanes: "pandas,polars,duckdb,spark,datafusion,dask",
            correctness_ref: "coverage_table correctness digest refs required",
            timing_ref: "source_read,compatibility_parse,vortex_import,write,reopen,scan,result_sink,evidence timing",
            environment_ref: "benchmark manifest with versions, hardware, OS, cache state, reproduction steps",
            execution_mode_ref: "docs/architecture/compute-engine-flow-reference.md",
            policy_ref: "fallback_attempted=false,external_engine_invoked=false,external_baseline_only",
            claim_gate_status: "not_claim_grade",
            missing_evidence: "pure_query_runtime_separation,reproducible_full_local_plus_spark_manifest,approved_comparison_rows,scale_evidence",
            external_baseline_only: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "Compatibility import rows prove workflow/evidence coverage, not Spark displacement or pure query speed.",
        },
        SparkDisplacementBenchmarkEvidenceRow {
            row_id: "prepared_native_runtime_lane",
            workload_family: "prepared_vortex_runtime",
            workload_ref: "selective filter, filter+projection+limit, group-by, hash join, top-N batch smoke",
            shardloom_lane: "prepared_vortex,native_vortex",
            baseline_oracle_lanes: "pandas,polars,duckdb,spark,datafusion,dask",
            correctness_ref: "operator correctness digest and execution certificate refs required",
            timing_ref: "prepared/native batch total, operator_compute, scan, materialization/decode timings",
            environment_ref: "profiled benchmark artifact with lane versions and source-state reuse fields",
            execution_mode_ref: "prepared_vortex and native_vortex mode refs",
            policy_ref: "no hidden fast mode; no external engine fallback",
            claim_gate_status: "not_claim_grade",
            missing_evidence: "claim_grade_rerun,complete_operator_coverage,materialization_decode_certificates,per_claim_attachment",
            external_baseline_only: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "Prepared/native rows are the runtime-development lane, not a public Spark replacement claim.",
        },
        SparkDisplacementBenchmarkEvidenceRow {
            row_id: "messy_data_etl_lane",
            workload_family: "messy_data_workflow",
            workload_ref: "dirty CSV cleanup, nested JSON scan, CDC overlay, result-sink replay",
            shardloom_lane: "compatibility_import_certified,prepared_vortex",
            baseline_oracle_lanes: "pandas,polars,duckdb,spark,datafusion,dask",
            correctness_ref: "workflow recipe refs, expected output refs, replay proof refs required",
            timing_ref: "workflow stage timing and output write/replay timing",
            environment_ref: "local taxonomy artifact profile metadata",
            execution_mode_ref: "mode attribution contract",
            policy_ref: "external baselines comparison-only; no object-store/table claim",
            claim_gate_status: "not_claim_grade",
            missing_evidence: "full_workflow_correctness_matrix,output_fanout_evidence,source_state_reuse_evidence,claim_grade_artifacts",
            external_baseline_only: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "Messy-data rows show scoped workflow coverage only.",
        },
        SparkDisplacementBenchmarkEvidenceRow {
            row_id: "scale_and_table_boundary_lane",
            workload_family: "scale_object_store_lakehouse",
            workload_ref: "larger-than-memory, split-parallel, object-store, table/lakehouse, distributed, Foundry scale boundaries",
            shardloom_lane: "report_only",
            baseline_oracle_lanes: "spark and managed-platform lanes as baselines/oracles only",
            correctness_ref: "scale correctness, split, spill, shuffle, commit, retry, and idempotency evidence required",
            timing_ref: "scale profile timing and resource evidence required",
            environment_ref: "declared resource envelope and scale benchmark profile required",
            execution_mode_ref: "GAR-SCALE-1 scale classes",
            policy_ref: "object_store_runtime=false,table_runtime=false,distributed_runtime=false",
            claim_gate_status: "not_claim_grade",
            missing_evidence: "scale_runtime,object_store_runtime,table_commit_runtime,distributed_runtime,foundry_scale_proof",
            external_baseline_only: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "Scale/table rows are boundary evidence only and cannot imply any-volume or Spark replacement support.",
        },
        SparkDisplacementBenchmarkEvidenceRow {
            row_id: "public_claim_attachment_lane",
            workload_family: "claim_attachment",
            workload_ref: "per-claim evidence attachment and release gate",
            shardloom_lane: "release_claim_gate",
            baseline_oracle_lanes: "external rows remain baseline-only",
            correctness_ref: "per-claim correctness refs required",
            timing_ref: "per-claim benchmark refs required",
            environment_ref: "benchmark manifest and reproducibility refs required",
            execution_mode_ref: "per-row execution_mode and engine_mode required",
            policy_ref: "no-fallback release gate and engine replacement claim inventory",
            claim_gate_status: "not_claim_grade",
            missing_evidence: "GAR-0041-A_per_claim_evidence_attachment,release_approval,publication_gate",
            external_baseline_only: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "No public performance, superiority, or Spark-displacement language is allowed without per-claim evidence.",
        },
    ]
}

#[must_use]
pub fn plan_benchmark_claim_evidence(
    scope: impl Into<String>,
    plan: &BenchmarkPlan,
) -> BenchmarkClaimEvidenceReport {
    let run_manifest = BenchmarkRunManifest::from_plan(plan);
    let comparison_report = BenchmarkComparisonReport::from_plan(plan);
    benchmark_claim_evidence_from_parts(scope, plan, &run_manifest, &comparison_report)
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn benchmark_claim_evidence_from_parts(
    scope: impl Into<String>,
    plan: &BenchmarkPlan,
    run_manifest: &BenchmarkRunManifest,
    comparison_report: &BenchmarkComparisonReport,
) -> BenchmarkClaimEvidenceReport {
    let bundle =
        BenchmarkEvidenceBundle::from_reports(run_manifest.clone(), comparison_report.clone());
    let claim_gate = bundle.claim_gate;
    let required_metrics = plan.required_metrics();
    let missing_required_foundation_metrics = plan
        .missing_required_foundation_metrics()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let missing_external_result_count = comparison_report
        .missing_results
        .iter()
        .filter(|gap| gap.engine != BaselineEngine::ShardLoom)
        .count();
    let missing_external_metric_count = comparison_report
        .missing_metrics
        .iter()
        .filter(|gap| gap.engine != BaselineEngine::ShardLoom)
        .count();
    let incomplete_dataset_profile_count = run_manifest
        .dataset_profiles
        .iter()
        .filter(|profile| !profile.is_complete())
        .count();
    let baselines_fallback_free = plan.baselines_are_fallback_free();
    let fallback_attempted = claim_gate.fallback.attempted();
    let performance_claim_allowed = bundle.can_publish_performance_claim();
    let measured_benchmark_result_rows_present =
        claim_gate.benchmark_evidence.is_present() && !comparison_report.results.is_empty();
    let reproducibility_manifest_populated = claim_gate.reproducibility_evidence.is_present();
    let approved_comparison_rows_present = comparison_report.missing_results.is_empty()
        && comparison_report.missing_metrics.is_empty()
        && missing_external_result_count == 0
        && !comparison_report.results.is_empty();
    let claim_grade_source_backed_benchmark_closeout_blocker_order =
        benchmark_claim_grade_closeout_blockers(
            measured_benchmark_result_rows_present,
            reproducibility_manifest_populated,
            approved_comparison_rows_present,
        );
    let claim_grade_source_backed_benchmark_closeout_allowed =
        claim_grade_source_backed_benchmark_closeout_blocker_order.is_empty();
    let blocked_surface_order =
        benchmark_claim_blocked_surfaces(&BenchmarkClaimBlockedSurfaceContext {
            plan,
            run_manifest,
            comparison_report,
            bundle: &bundle,
            missing_external_result_count,
            missing_external_metric_count,
            missing_foundation_metrics: !missing_required_foundation_metrics.is_empty(),
            baselines_fallback_free,
        });
    let blocked_surface_count = blocked_surface_order.len();
    let planned_surface_count =
        BenchmarkClaimEvidenceReport::surface_order().len() - blocked_surface_count;
    let status = if fallback_attempted || !baselines_fallback_free {
        BenchmarkClaimEvidenceStatus::UnsafeFallbackPolicy
    } else if performance_claim_allowed {
        BenchmarkClaimEvidenceStatus::ReadyForClaimReview
    } else {
        BenchmarkClaimEvidenceStatus::NeedsEvidence
    };

    BenchmarkClaimEvidenceReport {
        schema_version: "shardloom.benchmark_claim_evidence.v1",
        report_id: "cg6.benchmark_claim_evidence.aggregate",
        scope: scope.into(),
        status,
        scenario_count: plan.scenario_count(),
        scenario_name_order: plan
            .scenario_name_order()
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        workload_class_order: plan
            .workload_class_order()
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        required_metric_count: required_metrics.len(),
        required_metric_order: required_metrics
            .iter()
            .map(BenchmarkMetric::as_str)
            .map(ToString::to_string)
            .collect(),
        required_foundation_metric_count: BenchmarkPlan::required_foundation_metrics().len(),
        covered_required_foundation_metric_count: plan.covered_required_foundation_metric_count(),
        missing_required_foundation_metrics,
        baseline_count: plan.baseline_engines().len(),
        baseline_engine_order: plan
            .baseline_engine_order()
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        external_baseline_count: plan.external_baseline_count(),
        external_baseline_engine_order: plan
            .external_baseline_engine_order()
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        expected_result_count: plan.expected_result_count(),
        result_count: comparison_report.results.len(),
        missing_result_count: comparison_report.missing_results.len(),
        missing_external_result_count,
        missing_metric_count: comparison_report.missing_metrics.len(),
        run_manifest_status: run_manifest.status,
        run_manifest_emitted: true,
        missing_engine_version_count: run_manifest.missing_engine_versions.len(),
        dataset_profile_count: run_manifest.dataset_profiles.len(),
        incomplete_dataset_profile_count,
        reproduction_step_count: run_manifest.reproduction_steps.len(),
        cache_state: run_manifest.cache_state,
        comparison_report_status: comparison_report.status,
        comparison_report_emitted: true,
        correctness_evidence: claim_gate.correctness_evidence,
        benchmark_evidence: claim_gate.benchmark_evidence,
        required_metrics_evidence: claim_gate.required_metrics,
        comparison_report_evidence: claim_gate.comparison_report,
        reproducibility_evidence: claim_gate.reproducibility_evidence,
        claim_gate_status: claim_gate.status,
        planned_surface_count,
        blocked_surface_count,
        blocked_surface_order,
        claim_grade_source_backed_benchmark_closeout_required: true,
        claim_grade_source_backed_benchmark_closeout_allowed,
        claim_grade_source_backed_benchmark_closeout_blocker_order,
        measured_benchmark_result_rows_required: true,
        measured_benchmark_result_rows_present,
        reproducibility_manifest_population_required: true,
        reproducibility_manifest_populated,
        approved_comparison_rows_required: true,
        approved_comparison_rows_present,
        benchmark_execution_implemented: plan.benchmark_execution_implemented(),
        benchmark_execution_performed: false,
        external_engine_execution: false,
        query_execution: false,
        data_read: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        fallback_attempted,
        baselines_fallback_free,
        performance_claim_allowed,
        superiority_claim_allowed: false,
        best_default_claim_allowed: false,
        diagnostics: bundle.diagnostics,
    }
}

struct BenchmarkClaimBlockedSurfaceContext<'a> {
    plan: &'a BenchmarkPlan,
    run_manifest: &'a BenchmarkRunManifest,
    comparison_report: &'a BenchmarkComparisonReport,
    bundle: &'a BenchmarkEvidenceBundle,
    missing_external_result_count: usize,
    missing_external_metric_count: usize,
    missing_foundation_metrics: bool,
    baselines_fallback_free: bool,
}

fn benchmark_claim_blocked_surfaces(ctx: &BenchmarkClaimBlockedSurfaceContext<'_>) -> Vec<String> {
    let mut blocked = Vec::new();
    if ctx.plan.scenario_count() == 0 {
        blocked.push("benchmark_plan".to_string());
    }
    if ctx.plan.required_metrics().is_empty() || ctx.missing_foundation_metrics {
        blocked.push("required_metrics".to_string());
    }
    if !ctx.bundle.claim_gate.correctness_evidence.is_present() {
        blocked.push("correctness_evidence".to_string());
    }
    if !ctx.bundle.claim_gate.benchmark_evidence.is_present() {
        blocked.push("benchmark_result_rows".to_string());
    }
    if ctx.missing_external_result_count > 0 || ctx.missing_external_metric_count > 0 {
        blocked.push("external_comparison_results".to_string());
    }
    if ctx.comparison_report.scenario_count == 0 {
        blocked.push("comparison_report".to_string());
    }
    if !ctx.run_manifest.evidence_state().is_present() {
        blocked.push("reproducibility_manifest".to_string());
    }
    if ctx.bundle.claim_gate.fallback.attempted() || !ctx.baselines_fallback_free {
        blocked.push("no_fallback_policy".to_string());
    }
    if !ctx.bundle.can_publish_performance_claim() {
        blocked.push("claim_publication_gate".to_string());
    }
    blocked
}

fn benchmark_claim_grade_closeout_blockers(
    measured_benchmark_result_rows_present: bool,
    reproducibility_manifest_populated: bool,
    approved_comparison_rows_present: bool,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if !measured_benchmark_result_rows_present {
        blockers.push("measured_benchmark_result_rows_not_populated".to_string());
    }
    if !reproducibility_manifest_populated {
        blockers.push("reproducibility_manifest_not_populated".to_string());
    }
    if !approved_comparison_rows_present {
        blockers.push("approved_comparison_rows_missing".to_string());
    }
    blockers
}

fn benchmark_required_metric_sets_match(
    run_manifest: &BenchmarkRunManifest,
    comparison_report: &BenchmarkComparisonReport,
) -> bool {
    run_manifest.required_metrics.len() == comparison_report.required_metrics.len()
        && run_manifest
            .required_metrics
            .iter()
            .all(|metric| comparison_report.required_metrics.contains(metric))
}

fn benchmark_evidence_scenario_sets_match(
    run_manifest: &BenchmarkRunManifest,
    comparison_report: &BenchmarkComparisonReport,
) -> bool {
    if run_manifest.scenario_count != comparison_report.scenario_count {
        return false;
    }
    let manifest_names = benchmark_manifest_scenario_names(run_manifest);
    let comparison_names = benchmark_comparison_scenario_names(comparison_report);
    run_manifest.scenario_count == manifest_names.len()
        && comparison_report.scenario_count == comparison_names.len()
        && manifest_names == comparison_names
}

fn benchmark_manifest_scenario_names(run_manifest: &BenchmarkRunManifest) -> Vec<&str> {
    let mut names = Vec::new();
    for profile in &run_manifest.dataset_profiles {
        if !names.contains(&profile.scenario_name.as_str()) {
            names.push(profile.scenario_name.as_str());
        }
    }
    names.sort_unstable();
    names
}

fn benchmark_comparison_scenario_names(comparison_report: &BenchmarkComparisonReport) -> Vec<&str> {
    let mut names = Vec::new();
    for result in &comparison_report.results {
        if !names.contains(&result.scenario_name.as_str()) {
            names.push(result.scenario_name.as_str());
        }
    }
    for gap in &comparison_report.missing_results {
        if !names.contains(&gap.scenario_name.as_str()) {
            names.push(gap.scenario_name.as_str());
        }
    }
    for gap in &comparison_report.missing_metrics {
        if !names.contains(&gap.scenario_name.as_str()) {
            names.push(gap.scenario_name.as_str());
        }
    }
    names.sort_unstable();
    names
}

fn traditional_analytics_scenario(name: &str) -> BenchmarkScenario {
    let mut scenario = BenchmarkScenario::new(name, WorkloadClass::TraditionalAnalytics)
        .expect("valid traditional analytics benchmark scenario");
    scenario.dataset_name = Some("traditional_analytics_100m_rows".to_string());
    scenario.dataset_scale = Some("100m_rows_5gb_family".to_string());
    scenario.storage_format = Some("csv/parquet/vortex".to_string());
    scenario.query_or_operation = Some(name.replace(' ', "_"));
    for engine in [
        BaselineEngine::ShardLoom,
        BaselineEngine::Pandas,
        BaselineEngine::Polars,
        BaselineEngine::DuckDb,
        BaselineEngine::Spark,
        BaselineEngine::DataFusion,
        BaselineEngine::Dask,
    ] {
        scenario.add_baseline(engine);
    }
    scenario
}

fn source_backed_reader_chunk_scenario(name: &str, operation: &str) -> BenchmarkScenario {
    let mut scenario = BenchmarkScenario::new(name, WorkloadClass::SingleNodeEncodedExecution)
        .expect("valid source-backed reader-chunk benchmark scenario");
    scenario.dataset_name = Some("source-backed-edge-fixtures".to_string());
    scenario.dataset_scale = Some("tiny_reader_chunk_edges".to_string());
    scenario.storage_format = Some("vortex".to_string());
    scenario.query_or_operation = Some(operation.to_string());
    scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
    scenario.add_baseline(BaselineEngine::ShardLoom);
    scenario.add_baseline(BaselineEngine::VortexIntegration);
    for metric in [
        BenchmarkMetric::StartupLatencyMillis,
        BenchmarkMetric::WallTimeMillis,
        BenchmarkMetric::QueryRuntimeMillis,
        BenchmarkMetric::PeakMemoryBytes,
        BenchmarkMetric::BytesRead,
        BenchmarkMetric::BytesDecoded,
        BenchmarkMetric::BytesDecodeAvoided,
        BenchmarkMetric::RowsMaterialized,
        BenchmarkMetric::RowsMaterializationAvoided,
        BenchmarkMetric::SegmentsConsidered,
        BenchmarkMetric::SegmentsPruned,
        BenchmarkMetric::WorkAvoidedUnits,
        BenchmarkMetric::SpillRequiredBytes,
        BenchmarkMetric::SpillAvoidedBytes,
    ] {
        scenario.add_required_metric(metric);
    }
    scenario
}

/// Collection of benchmark scenarios for foundation planning.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkPlan {
    pub scenarios: Vec<BenchmarkScenario>,
}

impl Default for BenchmarkPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkPlan {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            scenarios: Vec::new(),
        }
    }

    pub fn add_scenario(&mut self, scenario: BenchmarkScenario) {
        self.scenarios.push(scenario);
    }

    #[must_use]
    pub fn scenario_count(&self) -> usize {
        self.scenarios.len()
    }

    /// Constructs a default foundation plan with placeholder scenarios.
    ///
    /// # Panics
    /// Panics only if hard-coded internal scenario names are invalid, which would
    /// indicate a programming error in this crate.
    #[must_use]
    pub fn default_foundation_plan() -> Self {
        let mut plan = Self::new();

        let mut scenario = BenchmarkScenario::new(
            "single-node encoded execution",
            WorkloadClass::SingleNodeEncodedExecution,
        )
        .expect("valid default scenario");
        scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::DataFusion);
        scenario.add_required_metric(BenchmarkMetric::StartupLatencyMillis);
        scenario.add_required_metric(BenchmarkMetric::WallTimeMillis);
        scenario.add_required_metric(BenchmarkMetric::QueryRuntimeMillis);
        scenario.add_required_metric(BenchmarkMetric::PeakMemoryBytes);
        scenario.add_required_metric(BenchmarkMetric::BytesRead);
        scenario.add_required_metric(BenchmarkMetric::BytesDecoded);
        scenario.add_required_metric(BenchmarkMetric::BytesDecodeAvoided);
        scenario.add_required_metric(BenchmarkMetric::RowsMaterializationAvoided);
        scenario.add_required_metric(BenchmarkMetric::SegmentsPruned);
        scenario.add_required_metric(BenchmarkMetric::WorkAvoidedUnits);
        scenario.add_required_metric(BenchmarkMetric::SpillRequiredBytes);
        scenario.add_required_metric(BenchmarkMetric::SpillAvoidedBytes);
        plan.add_scenario(scenario);

        plan.add_scenario(source_backed_reader_chunk_scenario(
            "source-backed dictionary reader chunk",
            "reader_chunk_dictionary_kernel_input",
        ));

        plan.add_scenario(source_backed_reader_chunk_scenario(
            "source-backed run-end reader chunk",
            "reader_chunk_run_end_kernel_input",
        ));

        let mut scenario = BenchmarkScenario::new(
            "massive object-store scan",
            WorkloadClass::MassiveObjectStoreScan,
        )
        .expect("valid default scenario");
        scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::Spark);
        scenario.add_required_metric(BenchmarkMetric::QueryRuntimeMillis);
        scenario.add_required_metric(BenchmarkMetric::PeakMemoryBytes);
        scenario.add_required_metric(BenchmarkMetric::BytesRead);
        scenario.add_required_metric(BenchmarkMetric::SegmentsConsidered);
        scenario.add_required_metric(BenchmarkMetric::SegmentsPruned);
        scenario.add_required_metric(BenchmarkMetric::ObjectStoreRequests);
        plan.add_scenario(scenario);

        let mut scenario = BenchmarkScenario::new(
            "incremental recomputation",
            WorkloadClass::IncrementalRecomputation,
        )
        .expect("valid default scenario");
        scenario.correctness_validation = CorrectnessValidationMode::PropertyBased;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::Polars);
        scenario.add_required_metric(BenchmarkMetric::RowsMaterialized);
        scenario.add_required_metric(BenchmarkMetric::RowsMaterializationAvoided);
        scenario.add_required_metric(BenchmarkMetric::WorkAvoidedUnits);
        scenario.add_required_metric(BenchmarkMetric::CostProxy);
        plan.add_scenario(scenario);

        let mut scenario = BenchmarkScenario::new(
            "native output and translation",
            WorkloadClass::NativeOutputAndTranslation,
        )
        .expect("valid default scenario");
        scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::VortexIntegration);
        scenario.add_required_metric(BenchmarkMetric::WriteCommitLatencyMillis);
        scenario.add_required_metric(BenchmarkMetric::BytesWritten);
        scenario.add_required_metric(BenchmarkMetric::OutputFiles);
        scenario.add_required_metric(BenchmarkMetric::OutputBytes);
        plan.add_scenario(scenario);

        let mut scenario = BenchmarkScenario::new(
            "failure and unsupported behavior",
            WorkloadClass::FailureAndUnsupportedBehavior,
        )
        .expect("valid default scenario");
        scenario.correctness_validation = CorrectnessValidationMode::UnsupportedDiagnosticOnly;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::Other);
        scenario.add_required_metric(BenchmarkMetric::SegmentsMetadataAnswered);
        plan.add_scenario(scenario);

        plan
    }

    /// Constructs the traditional single-node analytics benchmark plan.
    ///
    /// The plan is modeled after common dataframe/SQL benchmark tasks: ingest,
    /// filter, group-by aggregation, sort/top-k, join, and repeated-run
    /// measurement. External engines are comparison targets only and are never
    /// fallback execution paths.
    ///
    /// # Panics
    /// Panics only if hard-coded internal scenario names are invalid, which would
    /// indicate a programming error in this crate.
    #[must_use]
    pub fn traditional_analytics_plan() -> Self {
        let mut plan = Self::new();

        let mut ingest = traditional_analytics_scenario("csv/file ingest");
        ingest.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        for metric in [
            BenchmarkMetric::StartupLatencyMillis,
            BenchmarkMetric::WallTimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::BytesRead,
            BenchmarkMetric::RowsScanned,
            BenchmarkMetric::ObjectStoreRequests,
        ] {
            ingest.add_required_metric(metric);
        }
        plan.add_scenario(ingest);

        let mut filter = traditional_analytics_scenario("selective filter");
        filter.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        for metric in [
            BenchmarkMetric::QueryRuntimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::BytesRead,
            BenchmarkMetric::RowsScanned,
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::RowsMaterializationAvoided,
            BenchmarkMetric::BytesDecoded,
        ] {
            filter.add_required_metric(metric);
        }
        plan.add_scenario(filter);

        let mut aggregate = traditional_analytics_scenario("group by aggregation");
        aggregate.correctness_validation = CorrectnessValidationMode::DifferentialComparison;
        for metric in [
            BenchmarkMetric::QueryRuntimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::RowsScanned,
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::BytesDecoded,
            BenchmarkMetric::SpillRequiredBytes,
        ] {
            aggregate.add_required_metric(metric);
        }
        plan.add_scenario(aggregate);

        let mut sort = traditional_analytics_scenario("sort and top-k");
        sort.correctness_validation = CorrectnessValidationMode::DifferentialComparison;
        for metric in [
            BenchmarkMetric::QueryRuntimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::RowsScanned,
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::SpillRequiredBytes,
        ] {
            sort.add_required_metric(metric);
        }
        plan.add_scenario(sort);

        let mut join = traditional_analytics_scenario("hash join");
        join.correctness_validation = CorrectnessValidationMode::DifferentialComparison;
        for metric in [
            BenchmarkMetric::QueryRuntimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::RowsScanned,
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::BytesRead,
            BenchmarkMetric::SpillRequiredBytes,
        ] {
            join.add_required_metric(metric);
        }
        plan.add_scenario(join);

        plan
    }

    #[must_use]
    pub fn required_metrics(&self) -> Vec<BenchmarkMetric> {
        let mut metrics = Vec::new();
        for scenario in &self.scenarios {
            for metric in &scenario.required_metrics {
                if !metrics.contains(metric) {
                    metrics.push(*metric);
                }
            }
        }
        metrics
    }

    #[must_use]
    pub fn scenario_name_order(&self) -> Vec<&str> {
        self.scenarios
            .iter()
            .map(|scenario| scenario.name.as_str())
            .collect()
    }

    #[must_use]
    pub fn workload_class_order(&self) -> Vec<&'static str> {
        let mut classes = Vec::new();
        for scenario in &self.scenarios {
            let label = scenario.workload_class.as_str();
            if !classes.contains(&label) {
                classes.push(label);
            }
        }
        classes
    }

    #[must_use]
    pub fn correctness_validation_order(&self) -> Vec<&'static str> {
        let mut modes = Vec::new();
        for scenario in &self.scenarios {
            let label = scenario.correctness_validation.as_str();
            if !modes.contains(&label) {
                modes.push(label);
            }
        }
        modes
    }

    #[must_use]
    pub fn required_metric_order(&self) -> Vec<&'static str> {
        self.required_metrics()
            .iter()
            .map(BenchmarkMetric::as_str)
            .collect()
    }

    #[must_use]
    pub fn covers_metric(&self, metric: BenchmarkMetric) -> bool {
        self.scenarios
            .iter()
            .any(|scenario| scenario.requires_metric(metric))
    }

    #[must_use]
    pub fn covers_all_metrics(&self, metrics: &[BenchmarkMetric]) -> bool {
        metrics.iter().all(|metric| self.covers_metric(*metric))
    }

    #[must_use]
    pub fn baselines_are_fallback_free(&self) -> bool {
        self.scenarios
            .iter()
            .flat_map(|scenario| scenario.baselines.iter())
            .all(|baseline| !baseline.is_fallback_allowed())
    }

    #[must_use]
    pub fn baseline_engines(&self) -> Vec<BaselineEngine> {
        let mut engines = Vec::new();
        for scenario in &self.scenarios {
            for engine in &scenario.baselines {
                if !engines.contains(engine) {
                    engines.push(*engine);
                }
            }
        }
        engines
    }

    #[must_use]
    pub fn baseline_engine_order(&self) -> Vec<&'static str> {
        self.baseline_engines()
            .iter()
            .map(BaselineEngine::as_str)
            .collect()
    }

    #[must_use]
    pub fn external_baseline_engine_order(&self) -> Vec<&'static str> {
        self.baseline_engines()
            .iter()
            .filter(|engine| **engine != BaselineEngine::ShardLoom)
            .map(BaselineEngine::as_str)
            .collect()
    }

    #[must_use]
    pub fn external_baseline_count(&self) -> usize {
        self.external_baseline_engine_order().len()
    }

    #[must_use]
    pub fn expected_result_count(&self) -> usize {
        self.scenarios
            .iter()
            .map(|scenario| scenario.baselines.len())
            .sum()
    }

    #[must_use]
    pub fn scenario_with_correctness_validation_count(&self) -> usize {
        self.scenarios
            .iter()
            .filter(|scenario| {
                scenario.correctness_validation != CorrectnessValidationMode::NotYetDefined
            })
            .count()
    }

    #[must_use]
    pub fn scenario_with_required_metrics_count(&self) -> usize {
        self.scenarios
            .iter()
            .filter(|scenario| !scenario.required_metrics.is_empty())
            .count()
    }

    #[must_use]
    pub fn scenario_with_baselines_count(&self) -> usize {
        self.scenarios
            .iter()
            .filter(|scenario| !scenario.baselines.is_empty())
            .count()
    }

    #[must_use]
    pub fn required_foundation_metrics() -> &'static [BenchmarkMetric] {
        &[
            BenchmarkMetric::StartupLatencyMillis,
            BenchmarkMetric::WallTimeMillis,
            BenchmarkMetric::QueryRuntimeMillis,
            BenchmarkMetric::PeakMemoryBytes,
            BenchmarkMetric::BytesRead,
            BenchmarkMetric::BytesDecoded,
            BenchmarkMetric::BytesDecodeAvoided,
            BenchmarkMetric::RowsMaterializationAvoided,
            BenchmarkMetric::SegmentsPruned,
            BenchmarkMetric::WorkAvoidedUnits,
            BenchmarkMetric::SpillRequiredBytes,
            BenchmarkMetric::SpillAvoidedBytes,
            BenchmarkMetric::SegmentsConsidered,
            BenchmarkMetric::ObjectStoreRequests,
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::CostProxy,
            BenchmarkMetric::WriteCommitLatencyMillis,
            BenchmarkMetric::BytesWritten,
            BenchmarkMetric::OutputFiles,
            BenchmarkMetric::OutputBytes,
            BenchmarkMetric::SegmentsMetadataAnswered,
        ]
    }

    #[must_use]
    pub fn covered_required_foundation_metric_count(&self) -> usize {
        Self::required_foundation_metrics()
            .iter()
            .filter(|metric| self.covers_metric(**metric))
            .count()
    }

    #[must_use]
    pub fn missing_required_foundation_metrics(&self) -> Vec<&'static str> {
        Self::required_foundation_metrics()
            .iter()
            .filter(|metric| !self.covers_metric(**metric))
            .map(BenchmarkMetric::as_str)
            .collect()
    }

    #[must_use]
    pub fn required_foundation_metrics_covered(&self) -> bool {
        self.missing_required_foundation_metrics().is_empty()
    }

    #[must_use]
    pub fn runtime_metrics_covered(&self) -> bool {
        self.covers_all_metrics(&[
            BenchmarkMetric::WallTimeMillis,
            BenchmarkMetric::QueryRuntimeMillis,
        ])
    }

    #[must_use]
    pub fn peak_memory_metric_covered(&self) -> bool {
        self.covers_metric(BenchmarkMetric::PeakMemoryBytes)
    }

    #[must_use]
    pub fn bytes_read_written_metrics_covered(&self) -> bool {
        self.covers_all_metrics(&[BenchmarkMetric::BytesRead, BenchmarkMetric::BytesWritten])
    }

    #[must_use]
    pub fn startup_latency_metric_covered(&self) -> bool {
        self.covers_metric(BenchmarkMetric::StartupLatencyMillis)
    }

    #[must_use]
    pub fn query_runtime_metric_covered(&self) -> bool {
        self.covers_metric(BenchmarkMetric::QueryRuntimeMillis)
    }

    #[must_use]
    pub fn write_commit_latency_metric_covered(&self) -> bool {
        self.covers_metric(BenchmarkMetric::WriteCommitLatencyMillis)
    }

    #[must_use]
    pub fn spill_metrics_covered(&self) -> bool {
        self.covers_all_metrics(&[
            BenchmarkMetric::SpillRequiredBytes,
            BenchmarkMetric::SpillAvoidedBytes,
        ])
    }

    #[must_use]
    pub fn object_store_request_metric_covered(&self) -> bool {
        self.covers_metric(BenchmarkMetric::ObjectStoreRequests)
    }

    #[must_use]
    pub fn materialization_metrics_covered(&self) -> bool {
        self.covers_all_metrics(&[
            BenchmarkMetric::RowsMaterialized,
            BenchmarkMetric::RowsMaterializationAvoided,
        ])
    }

    #[must_use]
    pub const fn benchmark_execution_implemented(&self) -> bool {
        let _ = self;
        false
    }

    #[must_use]
    pub fn claim_gate(&self) -> BenchmarkClaimGate {
        BenchmarkClaimGate::new(
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            if self.required_metrics().is_empty() {
                BenchmarkEvidenceState::Missing
            } else {
                BenchmarkEvidenceState::Present
            },
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            BenchmarkFallbackState::NotAttempted,
        )
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut lines = vec![
            "benchmark foundation plan".to_string(),
            "benchmark execution is not implemented yet".to_string(),
            "baselines are comparison targets only".to_string(),
            "fallback execution: disabled".to_string(),
            format!("claim gate: {}", self.claim_gate().status.as_str()),
            format!("scenario count: {}", self.scenarios.len()),
        ];

        for scenario in &self.scenarios {
            lines.push("---".to_string());
            lines.push(scenario.to_human_text());
        }

        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_plan(name: &str, metric: BenchmarkMetric) -> BenchmarkPlan {
        let mut plan = BenchmarkPlan::new();
        let mut scenario = BenchmarkScenario::new(name, WorkloadClass::SingleNodeEncodedExecution)
            .expect("scenario");
        scenario.dataset_name = Some("fixture".to_string());
        scenario.dataset_scale = Some("tiny".to_string());
        scenario.storage_format = Some("vortex".to_string());
        scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_required_metric(metric);
        plan.add_scenario(scenario);
        plan
    }

    fn reproducible_manifest(plan: &BenchmarkPlan) -> BenchmarkRunManifest {
        let mut manifest = BenchmarkRunManifest::from_plan(plan);
        manifest.add_engine_version(
            BenchmarkEngineVersion::new(BaselineEngine::ShardLoom, "1.0.0")
                .expect("engine version"),
        );
        for profile in &mut manifest.dataset_profiles {
            profile.schema_profile = Some("u64 count fixture".to_string());
            profile.compression = Some("vortex".to_string());
        }
        manifest.hardware_profile = Some("local ci".to_string());
        manifest.operating_system_profile = Some("windows".to_string());
        manifest.runtime_configuration = Some("debug tests".to_string());
        manifest.cache_state = BenchmarkCacheState::Cold;
        manifest.add_reproduction_step("cargo test benchmark".to_string());
        manifest.correctness_evidence = BenchmarkEvidenceState::Present;
        manifest.refresh_against_plan(plan);
        manifest
    }

    fn ready_comparison(
        plan: &BenchmarkPlan,
        metric: BenchmarkMetric,
    ) -> BenchmarkComparisonReport {
        let scenario_name = plan.scenarios[0].name.clone();
        let mut result =
            BenchmarkResult::new(scenario_name, BaselineEngine::ShardLoom).expect("result");
        result.add_metric(metric, MetricValue::U64(1));
        BenchmarkComparisonReport::from_plan_and_results(
            plan,
            vec![result],
            BenchmarkEvidenceState::Present,
        )
    }

    fn ready_external_plan(name: &str, metric: BenchmarkMetric) -> BenchmarkPlan {
        let mut plan = BenchmarkPlan::new();
        let mut scenario = BenchmarkScenario::new(name, WorkloadClass::SingleNodeEncodedExecution)
            .expect("scenario");
        scenario.dataset_name = Some("fixture".to_string());
        scenario.dataset_scale = Some("tiny".to_string());
        scenario.storage_format = Some("vortex".to_string());
        scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
        scenario.add_baseline(BaselineEngine::ShardLoom);
        scenario.add_baseline(BaselineEngine::DuckDb);
        scenario.add_required_metric(metric);
        plan.add_scenario(scenario);
        plan
    }

    #[test]
    fn spark_fallback_not_allowed() {
        assert!(!BaselineEngine::Spark.is_fallback_allowed());
    }

    #[test]
    fn datafusion_fallback_not_allowed() {
        assert!(!BaselineEngine::DataFusion.is_fallback_allowed());
    }

    #[test]
    fn shardloom_fallback_not_allowed() {
        assert!(!BaselineEngine::ShardLoom.is_fallback_allowed());
    }

    #[test]
    fn benchmark_scenario_rejects_empty_name() {
        assert!(BenchmarkScenario::new("\n\t", WorkloadClass::LargeJoin).is_err());
    }

    #[test]
    fn benchmark_scenario_fallback_disallowed() {
        let scenario = BenchmarkScenario::new("test", WorkloadClass::LargeJoin).expect("valid");
        assert!(!scenario.fallback_execution_allowed());
    }

    #[test]
    fn benchmark_scenario_human_text_mentions_fallback_disabled() {
        let scenario = BenchmarkScenario::new("test", WorkloadClass::LargeJoin).expect("valid");
        assert!(
            scenario
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn benchmark_result_rejects_empty_scenario_name() {
        assert!(BenchmarkResult::new(" ", BaselineEngine::ShardLoom).is_err());
    }

    #[test]
    fn metric_value_known_unknown_works() {
        assert!(MetricValue::U64(1).is_known());
        assert!(MetricValue::F64(1.5).is_known());
        assert!(!MetricValue::Unknown.is_known());
    }

    #[test]
    fn evidence_bundle_rejects_required_metric_mismatch() {
        let manifest_plan = ready_plan("encoded count", BenchmarkMetric::WallTimeMillis);
        let comparison_plan = ready_plan("encoded count", BenchmarkMetric::BytesRead);
        let manifest = reproducible_manifest(&manifest_plan);
        let comparison = ready_comparison(&comparison_plan, BenchmarkMetric::BytesRead);
        assert!(manifest.status.is_reproducible());
        assert!(comparison.status.is_ready_for_comparison_review());

        let bundle = BenchmarkEvidenceBundle::from_reports(manifest, comparison);

        assert!(!bundle.can_publish_performance_claim());
        assert_eq!(
            bundle.claim_gate.required_metrics,
            BenchmarkEvidenceState::Missing
        );
        assert!(bundle.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("same scenario set and required metric set"))
        }));
    }

    #[test]
    fn evidence_bundle_rejects_scenario_set_mismatch() {
        let manifest_plan = ready_plan("encoded count", BenchmarkMetric::WallTimeMillis);
        let comparison_plan = ready_plan("metadata count", BenchmarkMetric::WallTimeMillis);
        let manifest = reproducible_manifest(&manifest_plan);
        let comparison = ready_comparison(&comparison_plan, BenchmarkMetric::WallTimeMillis);
        assert!(manifest.status.is_reproducible());
        assert!(comparison.status.is_ready_for_comparison_review());

        let bundle = BenchmarkEvidenceBundle::from_reports(manifest, comparison);

        assert!(!bundle.can_publish_performance_claim());
        assert_eq!(
            bundle.claim_gate.comparison_report,
            BenchmarkEvidenceState::Missing
        );
        assert!(bundle.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("same scenario set and required metric set"))
        }));
    }

    #[test]
    fn default_foundation_plan_has_at_least_five_scenarios() {
        let plan = BenchmarkPlan::default_foundation_plan();
        assert!(plan.scenarios.len() >= 5);
    }

    #[test]
    fn default_foundation_plan_exposes_coverage_inventory() {
        let plan = BenchmarkPlan::default_foundation_plan();

        assert_eq!(plan.scenario_count(), 7);
        assert_eq!(plan.required_metrics().len(), 21);
        assert_eq!(BenchmarkPlan::required_foundation_metrics().len(), 21);
        assert_eq!(plan.covered_required_foundation_metric_count(), 21);
        assert!(plan.required_foundation_metrics_covered());
        assert!(plan.missing_required_foundation_metrics().is_empty());
        assert_eq!(plan.scenario_with_correctness_validation_count(), 7);
        assert_eq!(plan.scenario_with_required_metrics_count(), 7);
        assert_eq!(plan.scenario_with_baselines_count(), 7);
        assert_eq!(plan.expected_result_count(), 14);
        assert_eq!(plan.external_baseline_count(), 5);
        assert_eq!(
            plan.baseline_engine_order(),
            vec![
                "shardloom",
                "datafusion",
                "vortex_integration",
                "spark",
                "polars",
                "other"
            ]
        );
        assert!(plan.runtime_metrics_covered());
        assert!(plan.peak_memory_metric_covered());
        assert!(plan.bytes_read_written_metrics_covered());
        assert!(plan.startup_latency_metric_covered());
        assert!(plan.query_runtime_metric_covered());
        assert!(plan.write_commit_latency_metric_covered());
        assert!(plan.spill_metrics_covered());
        assert!(plan.object_store_request_metric_covered());
        assert!(plan.materialization_metrics_covered());
        assert!(!plan.benchmark_execution_implemented());
    }

    #[test]
    fn plan_human_text_has_baseline_comparison_language() {
        let text = BenchmarkPlan::default_foundation_plan().to_human_text();
        assert!(text.contains("baselines are comparison targets only"));
    }

    #[test]
    fn traditional_analytics_plan_includes_dask_and_common_operations() {
        let plan = BenchmarkPlan::traditional_analytics_plan();

        assert_eq!(plan.scenario_count(), 5);
        assert_eq!(
            plan.scenario_name_order(),
            vec![
                "csv/file ingest",
                "selective filter",
                "group by aggregation",
                "sort and top-k",
                "hash join"
            ]
        );
        assert_eq!(
            plan.baseline_engine_order(),
            vec![
                "shardloom",
                "pandas",
                "polars",
                "duckdb",
                "spark",
                "datafusion",
                "dask"
            ]
        );
        assert_eq!(plan.workload_class_order(), vec!["traditional_analytics"]);
        assert!(plan.baselines_are_fallback_free());
        assert!(plan.runtime_metrics_covered());
        assert!(plan.peak_memory_metric_covered());
        assert!(plan.materialization_metrics_covered());
    }

    #[test]
    fn benchmark_claim_evidence_report_blocks_claims_without_result_rows() {
        let report =
            plan_benchmark_claim_evidence("foundation", &BenchmarkPlan::default_foundation_plan());

        assert_eq!(
            report.schema_version,
            "shardloom.benchmark_claim_evidence.v1"
        );
        assert_eq!(report.report_id, "cg6.benchmark_claim_evidence.aggregate");
        assert_eq!(report.status, BenchmarkClaimEvidenceStatus::NeedsEvidence);
        assert_eq!(report.scenario_count, 7);
        assert_eq!(report.required_metric_count, 21);
        assert_eq!(report.expected_result_count, 14);
        assert_eq!(report.result_count, 0);
        assert_eq!(report.missing_result_count, 14);
        assert!(report.missing_external_result_count > 0);
        assert_eq!(
            report.run_manifest_status,
            BenchmarkReproducibilityStatus::Incomplete
        );
        assert_eq!(
            report.comparison_report_status,
            BenchmarkComparisonStatus::EvidenceMissing
        );
        assert_eq!(report.correctness_evidence, BenchmarkEvidenceState::Missing);
        assert_eq!(report.benchmark_evidence, BenchmarkEvidenceState::Missing);
        assert_eq!(
            report.required_metrics_evidence,
            BenchmarkEvidenceState::Present
        );
        assert_eq!(
            report.reproducibility_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert_eq!(
            report.claim_gate_status,
            BenchmarkClaimStatus::EvidenceMissing
        );
        assert_eq!(
            report.blocked_surface_order,
            vec![
                "correctness_evidence",
                "benchmark_result_rows",
                "external_comparison_results",
                "reproducibility_manifest",
                "claim_publication_gate"
            ]
        );
        assert_eq!(report.blocked_surface_count, 5);
        assert_eq!(report.planned_surface_count, 4);
        assert!(report.claim_grade_source_backed_benchmark_closeout_required);
        assert!(!report.claim_grade_source_backed_benchmark_closeout_allowed);
        assert_eq!(
            report.claim_grade_source_backed_benchmark_closeout_blocker_order,
            vec![
                "measured_benchmark_result_rows_not_populated".to_string(),
                "reproducibility_manifest_not_populated".to_string(),
                "approved_comparison_rows_missing".to_string()
            ]
        );
        assert!(report.measured_benchmark_result_rows_required);
        assert!(!report.measured_benchmark_result_rows_present);
        assert!(report.reproducibility_manifest_population_required);
        assert!(!report.reproducibility_manifest_populated);
        assert!(report.approved_comparison_rows_required);
        assert!(!report.approved_comparison_rows_present);
        assert!(!report.performance_claim_allowed);
        assert!(!report.superiority_claim_allowed);
        assert!(!report.best_default_claim_allowed);
        assert!(report.baselines_fallback_free);
        assert!(!report.fallback_attempted);
        assert!(report.side_effect_free());
        assert!(
            report
                .to_human_text()
                .contains("fallback_execution=disabled")
        );
    }

    #[test]
    fn benchmark_claim_evidence_report_includes_traditional_baselines() {
        let report = plan_benchmark_claim_evidence(
            "traditional-analytics",
            &BenchmarkPlan::traditional_analytics_plan(),
        );

        assert_eq!(report.scope, "traditional-analytics");
        assert_eq!(report.scenario_count, 5);
        assert_eq!(report.expected_result_count, 35);
        assert_eq!(report.missing_result_count, 35);
        assert_eq!(report.missing_external_result_count, 30);
        assert_eq!(report.external_baseline_count, 6);
        assert_eq!(
            report.external_baseline_engine_order,
            vec!["pandas", "polars", "duckdb", "spark", "datafusion", "dask"]
        );
        assert!(report.side_effect_free());
        assert!(!report.performance_claim_allowed);
    }

    #[test]
    fn spark_displacement_benchmark_evidence_matrix_blocks_public_claims() {
        let report = plan_spark_displacement_benchmark_evidence_matrix();

        assert_eq!(
            report.schema_version,
            "shardloom.spark_displacement_benchmark_evidence_matrix.v1"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.support_status, "report_only");
        assert_eq!(report.rows.len(), 5);
        assert!(report.all_rows_not_claim_grade());
        assert!(report.all_external_lanes_baseline_only());
        assert!(report.side_effect_free());
        assert!(!report.performance_claim_allowed);
        assert!(!report.superiority_claim_allowed);
        assert!(!report.spark_displacement_claim_allowed);
        assert!(!report.benchmark_rerun_performed);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(report.row_order().contains(&"prepared_native_runtime_lane"));
        assert!(
            report
                .missing_evidence()
                .iter()
                .any(|missing| missing.contains("GAR-0041-A"))
        );
    }

    #[test]
    fn benchmark_claim_evidence_blocks_external_rows_missing_required_metrics() {
        let plan = ready_external_plan("encoded count", BenchmarkMetric::WallTimeMillis);
        let scenario_name = plan.scenarios[0].name.clone();
        let mut shardloom_result =
            BenchmarkResult::new(scenario_name.clone(), BaselineEngine::ShardLoom)
                .expect("shardloom result");
        shardloom_result.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(10));
        let external_result =
            BenchmarkResult::new(scenario_name, BaselineEngine::DuckDb).expect("external result");
        let comparison = BenchmarkComparisonReport::from_plan_and_results(
            &plan,
            vec![shardloom_result, external_result],
            BenchmarkEvidenceState::Present,
        );
        let manifest = BenchmarkRunManifest::from_plan(&plan);

        let report = benchmark_claim_evidence_from_parts(
            "external-missing-metric",
            &plan,
            &manifest,
            &comparison,
        );

        assert_eq!(report.missing_external_result_count, 0);
        assert_eq!(report.missing_metric_count, 1);
        assert!(
            report
                .blocked_surface_order
                .contains(&"external_comparison_results".to_string())
        );
    }

    #[test]
    fn benchmark_closeout_allowed_depends_on_closeout_blockers_only() {
        let manifest_plan = ready_plan("encoded count", BenchmarkMetric::WallTimeMillis);
        let comparison_plan = ready_plan("metadata count", BenchmarkMetric::WallTimeMillis);
        let manifest = reproducible_manifest(&manifest_plan);
        let comparison = ready_comparison(&comparison_plan, BenchmarkMetric::WallTimeMillis);

        let report = benchmark_claim_evidence_from_parts(
            "closeout-independent",
            &manifest_plan,
            &manifest,
            &comparison,
        );

        assert!(
            report
                .claim_grade_source_backed_benchmark_closeout_blocker_order
                .is_empty()
        );
        assert!(report.claim_grade_source_backed_benchmark_closeout_allowed);
        assert!(!report.performance_claim_allowed);
        assert!(
            report
                .blocked_surface_order
                .contains(&"claim_publication_gate".to_string())
        );
    }
}
