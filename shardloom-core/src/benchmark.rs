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
    ReadyForClaimReview,
}

impl BenchmarkComparisonStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvidenceMissing => "evidence_missing",
            Self::ReadyForClaimReview => "ready_for_claim_review",
        }
    }

    #[must_use]
    pub const fn is_ready_for_claim_review(&self) -> bool {
        matches!(self, Self::ReadyForClaimReview)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkClaimGate {
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub required_metrics: BenchmarkEvidenceState,
    pub comparison_report: BenchmarkEvidenceState,
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
        fallback: BenchmarkFallbackState,
    ) -> Self {
        let status = if correctness_evidence.is_present()
            && benchmark_evidence.is_present()
            && required_metrics.is_present()
            && comparison_report.is_present()
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
        let gate = BenchmarkClaimGate::new(
            correctness_evidence,
            benchmark_evidence,
            if required_metrics.is_empty() {
                BenchmarkEvidenceState::Missing
            } else {
                BenchmarkEvidenceState::Present
            },
            BenchmarkEvidenceState::Present,
            fallback,
        );
        let status = if gate.can_publish_performance_claim() {
            BenchmarkComparisonStatus::ReadyForClaimReview
        } else {
            BenchmarkComparisonStatus::EvidenceMissing
        };
        let mut diagnostics = Vec::new();
        if !status.is_ready_for_claim_review() {
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
    pub fn covers_metric(&self, metric: BenchmarkMetric) -> bool {
        self.scenarios
            .iter()
            .any(|scenario| scenario.requires_metric(metric))
    }

    #[must_use]
    pub fn baselines_are_fallback_free(&self) -> bool {
        self.scenarios
            .iter()
            .flat_map(|scenario| scenario.baselines.iter())
            .all(|baseline| !baseline.is_fallback_allowed())
    }

    #[must_use]
    pub fn expected_result_count(&self) -> usize {
        self.scenarios
            .iter()
            .map(|scenario| scenario.baselines.len())
            .sum()
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
    fn default_foundation_plan_has_at_least_five_scenarios() {
        let plan = BenchmarkPlan::default_foundation_plan();
        assert!(plan.scenarios.len() >= 5);
    }

    #[test]
    fn plan_human_text_has_baseline_comparison_language() {
        let text = BenchmarkPlan::default_foundation_plan().to_human_text();
        assert!(text.contains("baselines are comparison targets only"));
    }
}
