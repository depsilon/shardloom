use crate::{EstimateConfidence, PlanNodeId};
use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, EncodedSegment, Result, ShardLoomError,
};

/// Planning-only optimizer phases for the `ShardLoom` skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPhase {
    FrontendNormalization,
    Logical,
    VortexPhysical,
    RuntimeAdaptive,
    PostExecutionDiagnostics,
    Unsupported,
}
impl OptimizerPhase {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FrontendNormalization => "frontend_normalization",
            Self::Logical => "logical",
            Self::VortexPhysical => "vortex_physical",
            Self::RuntimeAdaptive => "runtime_adaptive",
            Self::PostExecutionDiagnostics => "post_execution_diagnostics",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptimizerRuleId(String);
impl OptimizerRuleId {
    /// Creates a validated optimizer rule identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for empty or whitespace-only values.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "optimizer rule id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleKind {
    PredicatePushdown,
    ProjectionPushdown,
    MetadataOnlyAnswer,
    SegmentPruning,
    EncodedKernelSelection,
    PartialDecodePlanning,
    LateMaterialization,
    RuntimeFilterPushdown,
    DynamicPruning,
    JoinStrategySelection,
    AggregateStrategySelection,
    SkewHandling,
    SinkDrivenPlanning,
    MemorySpillAwarePlanning,
    ObjectStorePlanning,
    Unsupported,
}
impl OptimizerRuleKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PredicatePushdown => "predicate_pushdown",
            Self::ProjectionPushdown => "projection_pushdown",
            Self::MetadataOnlyAnswer => "metadata_only_answer",
            Self::SegmentPruning => "segment_pruning",
            Self::EncodedKernelSelection => "encoded_kernel_selection",
            Self::PartialDecodePlanning => "partial_decode_planning",
            Self::LateMaterialization => "late_materialization",
            Self::RuntimeFilterPushdown => "runtime_filter_pushdown",
            Self::DynamicPruning => "dynamic_pruning",
            Self::JoinStrategySelection => "join_strategy_selection",
            Self::AggregateStrategySelection => "aggregate_strategy_selection",
            Self::SkewHandling => "skew_handling",
            Self::SinkDrivenPlanning => "sink_driven_planning",
            Self::MemorySpillAwarePlanning => "memory_spill_aware_planning",
            Self::ObjectStorePlanning => "object_store_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_vortex_native(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnlyAnswer
                | Self::SegmentPruning
                | Self::EncodedKernelSelection
                | Self::PartialDecodePlanning
                | Self::LateMaterialization
        )
    }
    #[must_use]
    pub const fn is_runtime_adaptive(&self) -> bool {
        matches!(
            self,
            Self::RuntimeFilterPushdown
                | Self::DynamicPruning
                | Self::SkewHandling
                | Self::MemorySpillAwarePlanning
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleStatus {
    Applied,
    NotApplicable,
    Deferred,
    Rejected,
    Unsupported,
}
impl OptimizerRuleStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotApplicable => "not_applicable",
            Self::Deferred => "deferred",
            Self::Rejected => "rejected",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected | Self::Unsupported)
    }
}

const EVIDENCE_AWARE_OPTIMIZER_SCHEMA_VERSION: &str = "shardloom.evidence_aware_optimizer_trace.v1";
const EVIDENCE_AWARE_OPTIMIZER_REPORT_ID: &str = "gar-perf-2b.evidence_aware_logical_optimizer";
const EVIDENCE_AWARE_OPTIMIZER_TRACE_ID: &str = "optimizer_trace.gar_perf_2b.report_only_registry";
const EVIDENCE_AWARE_OPTIMIZER_REGISTRY_VERSION: &str = "gar-perf-2b.optimizer_registry.v1";
const EVIDENCE_AWARE_OPTIMIZER_BENCHMARK_TRACE_REF: &str =
    "optimizer-trace://gar-perf-2b.report-only-registry";
const REPORT_ONLY_PLAN_DIGEST: &str = "not_emitted_report_only";
const REPORT_ONLY_CORRECTNESS_REF: &str = "not_required_no_rewrite_applied";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerTraceRuleFamily {
    PredicatePushdown,
    ProjectionPushdown,
    SliceLimitPushdown,
    CommonSubplanSourceStateReuse,
    ExpressionSimplification,
    ConstantFolding,
    TypeCoercion,
    JoinOrdering,
    CardinalityEstimation,
}

impl OptimizerTraceRuleFamily {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PredicatePushdown => "predicate_pushdown",
            Self::ProjectionPushdown => "projection_pushdown",
            Self::SliceLimitPushdown => "slice_limit_pushdown",
            Self::CommonSubplanSourceStateReuse => "common_subplan_source_state_reuse",
            Self::ExpressionSimplification => "expression_simplification",
            Self::ConstantFolding => "constant_folding",
            Self::TypeCoercion => "type_coercion",
            Self::JoinOrdering => "join_ordering",
            Self::CardinalityEstimation => "cardinality_estimation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerTraceRuleStatus {
    Admitted,
    Applied,
    Blocked,
    Unsupported,
    NotApplicable,
    ReportOnly,
}

impl OptimizerTraceRuleStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Applied => "applied",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
            Self::NotApplicable => "not_applicable",
            Self::ReportOnly => "report_only",
        }
    }

    #[must_use]
    pub const fn admitted(&self) -> bool {
        matches!(self, Self::Admitted | Self::Applied)
    }

    #[must_use]
    pub const fn applied(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRewriteSafetyStatus {
    ReportOnlyNoRewrite,
    CorrectnessProofRequired,
    BlockedUnsupportedSemantics,
}

impl OptimizerRewriteSafetyStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyNoRewrite => "report_only_no_rewrite",
            Self::CorrectnessProofRequired => "correctness_proof_required",
            Self::BlockedUnsupportedSemantics => "blocked_unsupported_semantics",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityEstimationStatus {
    NotNeeded,
    Unknown,
    StatisticsRequired,
    ReportOnly,
}

impl CardinalityEstimationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::Unknown => "unknown",
            Self::StatisticsRequired => "statistics_required",
            Self::ReportOnly => "report_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EvidenceAwareOptimizerRuleTrace {
    pub rule_id: String,
    pub optimizer_phase: OptimizerPhase,
    pub rule_family: OptimizerTraceRuleFamily,
    pub rule_status: OptimizerTraceRuleStatus,
    pub blocked_reason: String,
    pub before_plan_digest: String,
    pub after_plan_digest: String,
    pub rewrite_safety_status: OptimizerRewriteSafetyStatus,
    pub evidence_preserved: bool,
    pub no_fallback_preserved: bool,
    pub claim_boundary_preserved: bool,
    pub materialization_boundary_preserved: bool,
    pub source_state_reuse_admitted: bool,
    pub estimated_input_cardinality: String,
    pub estimated_output_cardinality: String,
    pub cardinality_estimation_status: CardinalityEstimationStatus,
    pub correctness_smoke_ref: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: String,
}

impl EvidenceAwareOptimizerRuleTrace {
    #[must_use]
    pub fn report_only(rule_id: &str, family: OptimizerTraceRuleFamily, reason: &str) -> Self {
        Self::new(
            rule_id,
            family,
            OptimizerTraceRuleStatus::ReportOnly,
            reason,
            OptimizerRewriteSafetyStatus::ReportOnlyNoRewrite,
            false,
            CardinalityEstimationStatus::NotNeeded,
        )
    }

    #[must_use]
    pub fn admitted(
        rule_id: &str,
        family: OptimizerTraceRuleFamily,
        reason: &str,
        source_state_reuse_admitted: bool,
    ) -> Self {
        Self::new(
            rule_id,
            family,
            OptimizerTraceRuleStatus::Admitted,
            reason,
            OptimizerRewriteSafetyStatus::CorrectnessProofRequired,
            source_state_reuse_admitted,
            CardinalityEstimationStatus::ReportOnly,
        )
    }

    #[must_use]
    pub fn blocked(rule_id: &str, family: OptimizerTraceRuleFamily, reason: &str) -> Self {
        Self::new(
            rule_id,
            family,
            OptimizerTraceRuleStatus::Blocked,
            reason,
            OptimizerRewriteSafetyStatus::BlockedUnsupportedSemantics,
            false,
            CardinalityEstimationStatus::Unknown,
        )
    }

    #[must_use]
    pub fn unsupported(rule_id: &str, family: OptimizerTraceRuleFamily, reason: &str) -> Self {
        Self::new(
            rule_id,
            family,
            OptimizerTraceRuleStatus::Unsupported,
            reason,
            OptimizerRewriteSafetyStatus::BlockedUnsupportedSemantics,
            false,
            CardinalityEstimationStatus::Unknown,
        )
    }

    #[must_use]
    pub fn not_applicable(rule_id: &str, family: OptimizerTraceRuleFamily, reason: &str) -> Self {
        Self::new(
            rule_id,
            family,
            OptimizerTraceRuleStatus::NotApplicable,
            reason,
            OptimizerRewriteSafetyStatus::ReportOnlyNoRewrite,
            false,
            CardinalityEstimationStatus::NotNeeded,
        )
    }

    #[must_use]
    fn new(
        rule_id: &str,
        family: OptimizerTraceRuleFamily,
        status: OptimizerTraceRuleStatus,
        reason: &str,
        rewrite_safety_status: OptimizerRewriteSafetyStatus,
        source_state_reuse_admitted: bool,
        cardinality_estimation_status: CardinalityEstimationStatus,
    ) -> Self {
        Self {
            rule_id: rule_id.to_string(),
            optimizer_phase: OptimizerPhase::Logical,
            rule_family: family,
            rule_status: status,
            blocked_reason: reason.to_string(),
            before_plan_digest: REPORT_ONLY_PLAN_DIGEST.to_string(),
            after_plan_digest: REPORT_ONLY_PLAN_DIGEST.to_string(),
            rewrite_safety_status,
            evidence_preserved: true,
            no_fallback_preserved: true,
            claim_boundary_preserved: true,
            materialization_boundary_preserved: true,
            source_state_reuse_admitted,
            estimated_input_cardinality: "not_estimated_report_only".to_string(),
            estimated_output_cardinality: "not_estimated_report_only".to_string(),
            cardinality_estimation_status,
            correctness_smoke_ref: REPORT_ONLY_CORRECTNESS_REF.to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EvidenceAwareOptimizerTraceReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub optimizer_trace_id: String,
    pub optimizer_registry_version: String,
    pub optimizer_phase: OptimizerPhase,
    pub rule_rows: Vec<EvidenceAwareOptimizerRuleTrace>,
    pub benchmark_trace_ref: String,
    pub support_status: String,
    pub claim_gate_status: String,
    pub runtime_execution: bool,
    pub optimizer_execution: bool,
    pub plan_rewritten: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub write_io: bool,
    pub object_store_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub performance_claim_allowed: bool,
    pub broad_sql_dataframe_claim_allowed: bool,
}

impl EvidenceAwareOptimizerTraceReport {
    #[must_use]
    pub fn gar_perf_2b_report_only() -> Self {
        Self {
            schema_version: EVIDENCE_AWARE_OPTIMIZER_SCHEMA_VERSION,
            report_id: EVIDENCE_AWARE_OPTIMIZER_REPORT_ID.to_string(),
            optimizer_trace_id: EVIDENCE_AWARE_OPTIMIZER_TRACE_ID.to_string(),
            optimizer_registry_version: EVIDENCE_AWARE_OPTIMIZER_REGISTRY_VERSION.to_string(),
            optimizer_phase: OptimizerPhase::Logical,
            rule_rows: vec![
                EvidenceAwareOptimizerRuleTrace::report_only(
                    "predicate_pushdown",
                    OptimizerTraceRuleFamily::PredicatePushdown,
                    "predicate pushdown classification is visible, but no general logical rewrite is applied by this report-only trace",
                ),
                EvidenceAwareOptimizerRuleTrace::report_only(
                    "projection_pushdown",
                    OptimizerTraceRuleFamily::ProjectionPushdown,
                    "projection pushdown classification is visible, but no broad projection rewrite is applied by this report-only trace",
                ),
                EvidenceAwareOptimizerRuleTrace::blocked(
                    "slice_limit_pushdown",
                    OptimizerTraceRuleFamily::SliceLimitPushdown,
                    "limit/order/null semantics need scoped correctness smoke before this rewrite can be applied",
                ),
                EvidenceAwareOptimizerRuleTrace::admitted(
                    "common_subplan_source_state_reuse",
                    OptimizerTraceRuleFamily::CommonSubplanSourceStateReuse,
                    "scoped source-state reuse evidence exists for prepared/native benchmark batches, but this optimizer trace does not rewrite plans",
                    true,
                ),
                EvidenceAwareOptimizerRuleTrace::unsupported(
                    "expression_simplification",
                    OptimizerTraceRuleFamily::ExpressionSimplification,
                    "expression simplification lacks a stable expression IR rewrite proof in this slice",
                ),
                EvidenceAwareOptimizerRuleTrace::unsupported(
                    "constant_folding",
                    OptimizerTraceRuleFamily::ConstantFolding,
                    "constant folding lacks a stable expression IR rewrite proof in this slice",
                ),
                EvidenceAwareOptimizerRuleTrace::blocked(
                    "type_coercion",
                    OptimizerTraceRuleFamily::TypeCoercion,
                    "type coercion rewrites need semantic-profile and null/error behavior proof",
                ),
                EvidenceAwareOptimizerRuleTrace::blocked(
                    "join_ordering",
                    OptimizerTraceRuleFamily::JoinOrdering,
                    "join ordering needs cardinality, memory, spill, and correctness evidence before rewrite admission",
                ),
                EvidenceAwareOptimizerRuleTrace::not_applicable(
                    "cardinality_estimation",
                    OptimizerTraceRuleFamily::CardinalityEstimation,
                    "no input plan is supplied to this report-only registry snapshot, so cardinality estimation is not run",
                ),
            ],
            benchmark_trace_ref: EVIDENCE_AWARE_OPTIMIZER_BENCHMARK_TRACE_REF.to_string(),
            support_status: "report_only".to_string(),
            claim_gate_status: "not_claim_grade".to_string(),
            runtime_execution: false,
            optimizer_execution: false,
            plan_rewritten: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            write_io: false,
            object_store_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            performance_claim_allowed: false,
            broad_sql_dataframe_claim_allowed: false,
        }
    }

    #[must_use]
    pub fn rule_row_order(&self) -> Vec<&str> {
        self.rule_rows
            .iter()
            .map(|row| row.rule_id.as_str())
            .collect()
    }

    #[must_use]
    pub fn rule_status_vocabulary() -> [&'static str; 6] {
        [
            "admitted",
            "applied",
            "blocked",
            "unsupported",
            "not_applicable",
            "report_only",
        ]
    }

    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.rule_rows.len()
    }

    #[must_use]
    pub fn applied_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status.applied())
            .count()
    }

    #[must_use]
    pub fn admitted_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status.admitted())
            .count()
    }

    #[must_use]
    pub fn blocked_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status == OptimizerTraceRuleStatus::Blocked)
            .count()
    }

    #[must_use]
    pub fn unsupported_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status == OptimizerTraceRuleStatus::Unsupported)
            .count()
    }

    #[must_use]
    pub fn report_only_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status == OptimizerTraceRuleStatus::ReportOnly)
            .count()
    }

    #[must_use]
    pub fn not_applicable_rule_count(&self) -> usize {
        self.rule_rows
            .iter()
            .filter(|row| row.rule_status == OptimizerTraceRuleStatus::NotApplicable)
            .count()
    }

    #[must_use]
    pub fn all_no_fallback_no_external_engine(&self) -> bool {
        !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
            && self
                .rule_rows
                .iter()
                .all(|row| !row.fallback_attempted && !row.external_engine_invoked)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "evidence-aware optimizer trace\nschema_version: {}\ntrace: {}\nregistry: {}\nphase: {}\nrules: {}\napplied rewrites: {}\nclaim gate: {}\nfallback attempted: false\nexternal engine invoked: false",
            self.schema_version,
            self.optimizer_trace_id,
            self.optimizer_registry_version,
            self.optimizer_phase.as_str(),
            self.rule_count(),
            self.applied_rule_count(),
            self.claim_gate_status
        )
    }
}

#[must_use]
pub fn plan_evidence_aware_optimizer_trace() -> EvidenceAwareOptimizerTraceReport {
    EvidenceAwareOptimizerTraceReport::gar_perf_2b_report_only()
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptimizerRuleDecision {
    pub rule_id: OptimizerRuleId,
    pub phase: OptimizerPhase,
    pub kind: OptimizerRuleKind,
    pub status: OptimizerRuleStatus,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl OptimizerRuleDecision {
    #[must_use]
    pub fn applied(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::Applied,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn not_applicable(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::NotApplicable,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn deferred(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::Deferred,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::Unsupported,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some(
                    "Native optimizer rule execution is not implemented in this skeleton."
                        .to_string(),
                ),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "rule={} phase={} kind={} status={} fallback_execution=disabled",
            self.rule_id.as_str(),
            self.phase.as_str(),
            self.kind.as_str(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostMetric {
    Rows,
    EncodedBytes,
    DecodedBytes,
    MaterializedRows,
    ObjectStoreRequests,
    MemoryBytes,
    SpillBytes,
    ShuffleBytes,
    OutputBytes,
    EffectCalls,
    Unknown,
}
impl CostMetric {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::EncodedBytes => "encoded_bytes",
            Self::DecodedBytes => "decoded_bytes",
            Self::MaterializedRows => "materialized_rows",
            Self::ObjectStoreRequests => "object_store_requests",
            Self::MemoryBytes => "memory_bytes",
            Self::SpillBytes => "spill_bytes",
            Self::ShuffleBytes => "shuffle_bytes",
            Self::OutputBytes => "output_bytes",
            Self::EffectCalls => "effect_calls",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CostValue {
    Known(u64),
    Estimated(u64),
    Unknown,
}
impl CostValue {
    #[must_use]
    pub const fn known(value: u64) -> Self {
        Self::Known(value)
    }
    #[must_use]
    pub const fn estimated(value: u64) -> Self {
        Self::Estimated(value)
    }
    #[must_use]
    pub const fn unknown() -> Self {
        Self::Unknown
    }
    #[must_use]
    pub const fn is_known_or_estimated(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
    #[must_use]
    pub const fn unwrap_or(&self, fallback: u64) -> u64 {
        match self {
            Self::Known(v) | Self::Estimated(v) => *v,
            Self::Unknown => fallback,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::Known(v) => format!("known:{v}"),
            Self::Estimated(v) => format!("estimated:{v}"),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    pub metric: CostMetric,
    pub value: CostValue,
    pub confidence: EstimateConfidence,
    pub reason: Option<String>,
}
impl CostEstimate {
    #[must_use]
    pub fn new(metric: CostMetric, value: CostValue, confidence: EstimateConfidence) -> Self {
        Self {
            metric,
            value,
            confidence,
            reason: None,
        }
    }
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}={} confidence={}",
            self.metric.as_str(),
            self.value.summary(),
            self.confidence.as_str()
        )
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct CostModelInput {
    pub row_count: Option<u64>,
    pub encoded_bytes: Option<u64>,
    pub estimated_decoded_bytes: Option<u64>,
    pub selected_column_count: Option<usize>,
    pub total_column_count: Option<usize>,
    pub segment_count: Option<usize>,
    pub has_statistics: bool,
    pub has_byte_ranges: bool,
    pub streaming_required: bool,
    pub spill_supported: bool,
    pub native_vortex_output: bool,
}
impl CostModelInput {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            row_count: None,
            encoded_bytes: None,
            estimated_decoded_bytes: None,
            selected_column_count: None,
            total_column_count: None,
            segment_count: None,
            has_statistics: false,
            has_byte_ranges: false,
            streaming_required: false,
            spill_supported: false,
            native_vortex_output: true,
        }
    }
    #[must_use]
    pub fn from_segment(segment: &EncodedSegment) -> Self {
        Self {
            row_count: segment.stats.row_count,
            encoded_bytes: segment.layout.physical_size_bytes,
            estimated_decoded_bytes: None,
            selected_column_count: Some(1),
            total_column_count: Some(1),
            segment_count: Some(1),
            has_statistics: segment.can_use_metadata(),
            has_byte_ranges: segment.has_byte_ranges(),
            streaming_required: false,
            spill_supported: false,
            native_vortex_output: true,
        }
    }
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn projection_ratio(&self) -> Option<f64> {
        match (self.selected_column_count, self.total_column_count) {
            (Some(s), Some(t)) if t > 0 => Some(s as f64 / t as f64),
            _ => None,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "rows={:?} encoded_bytes={:?} has_statistics={} has_byte_ranges={} native_vortex_output={}",
            self.row_count,
            self.encoded_bytes,
            self.has_statistics,
            self.has_byte_ranges,
            self.native_vortex_output
        )
    }
}

// omitted: define remaining types similarly
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterKind {
    BloomLike,
    DictionaryId,
    Range,
    Constant,
    NullAware,
    SemiJoinReduction,
    DynamicPartition,
    Unsupported,
}
impl RuntimeFilterKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BloomLike => "bloom_like",
            Self::DictionaryId => "dictionary_id",
            Self::Range => "range",
            Self::Constant => "constant",
            Self::NullAware => "null_aware",
            Self::SemiJoinReduction => "semi_join_reduction",
            Self::DynamicPartition => "dynamic_partition",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_conservative_candidate(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterStatus {
    Planned,
    Generated,
    Applied,
    Rejected,
    Unsupported,
}
impl RuntimeFilterStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Generated => "generated",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeFilter {
    pub kind: RuntimeFilterKind,
    pub status: RuntimeFilterStatus,
    pub source_node: Option<PlanNodeId>,
    pub target_node: Option<PlanNodeId>,
    pub conservative: bool,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl RuntimeFilter {
    #[must_use]
    pub fn planned(kind: RuntimeFilterKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            status: RuntimeFilterStatus::Planned,
            source_node: None,
            target_node: None,
            conservative: kind.is_conservative_candidate(),
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        kind: RuntimeFilterKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            kind,
            status: RuntimeFilterStatus::Unsupported,
            source_node: None,
            target_node: None,
            conservative: false,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Runtime filters are planning metadata only in this skeleton.".to_string()),
            )],
        }
    }
    #[must_use]
    pub fn with_source_node(mut self, source_node: PlanNodeId) -> Self {
        self.source_node = Some(source_node);
        self
    }
    #[must_use]
    pub fn with_target_node(mut self, target_node: PlanNodeId) -> Self {
        self.target_node = Some(target_node);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "runtime_filter kind={} status={} conservative={} (planning metadata only)",
            self.kind.as_str(),
            self.status.as_str(),
            self.conservative
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicPruningDecision {
    NotNeeded { reason: String },
    Candidate { reason: String },
    Applied { reason: String },
    Rejected { reason: String },
    Unsupported { reason: String },
}
impl DynamicPruningDecision {
    #[must_use]
    pub fn reason(&self) -> &str {
        match self {
            Self::NotNeeded { reason }
            | Self::Candidate { reason }
            | Self::Applied { reason }
            | Self::Rejected { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected { .. } | Self::Unsupported { .. })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "dynamic_pruning={} reason={}",
            match self {
                Self::NotNeeded { .. } => "not_needed",
                Self::Candidate { .. } => "candidate",
                Self::Applied { .. } => "applied",
                Self::Rejected { .. } => "rejected",
                Self::Unsupported { .. } => "unsupported",
            },
            self.reason()
        )
    }
}

// ... keep concise
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveTriggerKind {
    RuntimeRowCount,
    RuntimeBytesRead,
    MemoryPressure,
    SpillPressure,
    SkewDetected,
    SinkBackpressure,
    RuntimeFilterAvailable,
    ObjectStoreLatency,
    Unknown,
}
impl AdaptiveTriggerKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RuntimeRowCount => "runtime_row_count",
            Self::RuntimeBytesRead => "runtime_bytes_read",
            Self::MemoryPressure => "memory_pressure",
            Self::SpillPressure => "spill_pressure",
            Self::SkewDetected => "skew_detected",
            Self::SinkBackpressure => "sink_backpressure",
            Self::RuntimeFilterAvailable => "runtime_filter_available",
            Self::ObjectStoreLatency => "object_store_latency",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveTrigger {
    pub kind: AdaptiveTriggerKind,
    pub reason: String,
}
impl AdaptiveTrigger {
    #[must_use]
    pub fn new(kind: AdaptiveTriggerKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("trigger={} reason={}", self.kind.as_str(), self.reason)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    ReduceParallelism,
    IncreaseParallelism,
    CoalesceTasks,
    SplitTasks,
    ApplyRuntimeFilter,
    ReplanJoin,
    ReplanAggregation,
    TriggerSpill,
    ChangeMaterialization,
    NoChange,
    Unsupported,
}
impl AdaptiveDecisionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReduceParallelism => "reduce_parallelism",
            Self::IncreaseParallelism => "increase_parallelism",
            Self::CoalesceTasks => "coalesce_tasks",
            Self::SplitTasks => "split_tasks",
            Self::ApplyRuntimeFilter => "apply_runtime_filter",
            Self::ReplanJoin => "replan_join",
            Self::ReplanAggregation => "replan_aggregation",
            Self::TriggerSpill => "trigger_spill",
            Self::ChangeMaterialization => "change_materialization",
            Self::NoChange => "no_change",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn changes_plan(&self) -> bool {
        !matches!(self, Self::NoChange | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveExecutionDecision {
    pub trigger: AdaptiveTrigger,
    pub kind: AdaptiveDecisionKind,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl AdaptiveExecutionDecision {
    #[must_use]
    pub fn new(
        trigger: AdaptiveTrigger,
        kind: AdaptiveDecisionKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            trigger,
            kind,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        trigger: AdaptiveTrigger,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            trigger,
            kind: AdaptiveDecisionKind::Unsupported,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some(
                    "Adaptive execution behavior is not implemented in this skeleton.".to_string(),
                ),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub const fn changes_plan(&self) -> bool {
        self.kind.changes_plan()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        matches!(self.kind, AdaptiveDecisionKind::Unsupported)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "adaptive kind={} trigger={} fallback_execution=disabled",
            self.kind.as_str(),
            self.trigger.kind.as_str()
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    BroadcastSmallSide,
    HashJoin,
    SortMergeJoin,
    RuntimeFilteredJoin,
    SemiJoinReduction,
    RangeAwareJoin,
    DictionaryAwareJoin,
    Unsupported,
}
impl JoinStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BroadcastSmallSide => "broadcast_small_side",
            Self::HashJoin => "hash_join",
            Self::SortMergeJoin => "sort_merge_join",
            Self::RuntimeFilteredJoin => "runtime_filtered_join",
            Self::SemiJoinReduction => "semi_join_reduction",
            Self::RangeAwareJoin => "range_aware_join",
            Self::DictionaryAwareJoin => "dictionary_aware_join",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_shuffle(&self) -> bool {
        matches!(self, Self::HashJoin | Self::SortMergeJoin)
    }
    #[must_use]
    pub const fn is_encoded_friendly(&self) -> bool {
        matches!(
            self,
            Self::RuntimeFilteredJoin
                | Self::SemiJoinReduction
                | Self::RangeAwareJoin
                | Self::DictionaryAwareJoin
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateStrategy {
    MetadataOnly,
    SegmentLocalPartial,
    EncodedAggregate,
    HashAggregate,
    SortAggregate,
    SpillableAggregate,
    StreamingAggregate,
    Unsupported,
}
impl AggregateStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::SegmentLocalPartial => "segment_local_partial",
            Self::EncodedAggregate => "encoded_aggregate",
            Self::HashAggregate => "hash_aggregate",
            Self::SortAggregate => "sort_aggregate",
            Self::SpillableAggregate => "spillable_aggregate",
            Self::StreamingAggregate => "streaming_aggregate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_vortex_native(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnly | Self::SegmentLocalPartial | Self::EncodedAggregate
        )
    }
    #[must_use]
    pub const fn may_require_spill(&self) -> bool {
        matches!(
            self,
            Self::HashAggregate | Self::SortAggregate | Self::SpillableAggregate
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewSignalKind {
    SegmentRowCount,
    SegmentByteSize,
    KeyFrequency,
    DictionarySkew,
    RuntimePartitionSize,
    TaskDuration,
    SpillPressure,
    ObjectStoreLatency,
    Unknown,
}
impl SkewSignalKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentRowCount => "segment_row_count",
            Self::SegmentByteSize => "segment_byte_size",
            Self::KeyFrequency => "key_frequency",
            Self::DictionarySkew => "dictionary_skew",
            Self::RuntimePartitionSize => "runtime_partition_size",
            Self::TaskDuration => "task_duration",
            Self::SpillPressure => "spill_pressure",
            Self::ObjectStoreLatency => "object_store_latency",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
    Unknown,
}
impl SkewSeverity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_action(&self) -> bool {
        matches!(self, Self::High | Self::Critical)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SkewSignal {
    pub kind: SkewSignalKind,
    pub severity: SkewSeverity,
    pub reason: String,
}
impl SkewSignal {
    #[must_use]
    pub fn new(kind: SkewSignalKind, severity: SkewSeverity, reason: impl Into<String>) -> Self {
        Self {
            kind,
            severity,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "skew={} severity={} reason={}",
            self.kind.as_str(),
            self.severity.as_str(),
            self.reason
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewHandlingStrategy {
    None,
    SplitLargeSegments,
    SplitHotKeys,
    BroadcastSmallSide,
    RangePartition,
    SkewAwareScheduling,
    IsolateSpillHeavyPartition,
    DynamicRepartition,
    Unsupported,
}
impl SkewHandlingStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SplitLargeSegments => "split_large_segments",
            Self::SplitHotKeys => "split_hot_keys",
            Self::BroadcastSmallSide => "broadcast_small_side",
            Self::RangePartition => "range_partition",
            Self::SkewAwareScheduling => "skew_aware_scheduling",
            Self::IsolateSpillHeavyPartition => "isolate_spill_heavy_partition",
            Self::DynamicRepartition => "dynamic_repartition",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_repartition(&self) -> bool {
        matches!(self, Self::RangePartition | Self::DynamicRepartition)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPlanStatus {
    Planned,
    PartiallyPlanned,
    Unsupported,
    NotImplemented,
}
impl OptimizerPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::PartiallyPlanned => "partially_planned",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported | Self::NotImplemented)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OptimizerPlanSkeleton {
    pub status: OptimizerPlanStatus,
    pub phase: OptimizerPhase,
    pub rule_decisions: Vec<OptimizerRuleDecision>,
    pub cost_estimates: Vec<CostEstimate>,
    pub runtime_filters: Vec<RuntimeFilter>,
    pub adaptive_decisions: Vec<AdaptiveExecutionDecision>,
    pub skew_signals: Vec<SkewSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl OptimizerPlanSkeleton {
    #[must_use]
    pub fn new(phase: OptimizerPhase) -> Self {
        Self {
            status: OptimizerPlanStatus::Planned,
            phase,
            rule_decisions: Vec::new(),
            cost_estimates: Vec::new(),
            runtime_filters: Vec::new(),
            adaptive_decisions: Vec::new(),
            skew_signals: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn not_implemented(
        phase: OptimizerPhase,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(phase);
        s.status = OptimizerPlanStatus::NotImplemented;
        s.add_diagnostic(Diagnostic::not_implemented(
            feature,
            reason,
            "Use planner skeleton outputs only; execution is not performed.",
        ));
        s
    }
    #[must_use]
    pub fn unsupported(
        phase: OptimizerPhase,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(phase);
        s.status = OptimizerPlanStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("No fallback execution is allowed.".to_string()),
        ));
        s
    }
    pub fn add_rule_decision(&mut self, decision: OptimizerRuleDecision) {
        self.rule_decisions.push(decision);
    }
    pub fn add_cost_estimate(&mut self, estimate: CostEstimate) {
        self.cost_estimates.push(estimate);
    }
    pub fn add_runtime_filter(&mut self, filter: RuntimeFilter) {
        self.runtime_filters.push(filter);
    }
    pub fn add_adaptive_decision(&mut self, decision: AdaptiveExecutionDecision) {
        self.adaptive_decisions.push(decision);
    }
    pub fn add_skew_signal(&mut self, signal: SkewSignal) {
        self.skew_signals.push(signal);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .rule_decisions
                .iter()
                .any(OptimizerRuleDecision::has_errors)
            || self.runtime_filters.iter().any(RuntimeFilter::has_errors)
            || self
                .adaptive_decisions
                .iter()
                .any(AdaptiveExecutionDecision::has_errors)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "optimizer plan status={} phase={}\nplanning skeleton only (no optimization execution performed)\nfallback execution: disabled",
            self.status.as_str(),
            self.phase.as_str()
        )
    }
}

const ADAPTIVE_MEMORY_SCHEMA_VERSION: &str = "shardloom.adaptive_optimizer_memory.v1";
const ADAPTIVE_MEMORY_REPORT_ID: &str = "cg14.adaptive-optimizer-memory";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveOptimizerMemoryStatus {
    ReportOnlyPlanned,
    BlockedByUnsafeRuntimeFilter,
    BlockedByMemoryBoundary,
    Unsupported,
}

impl AdaptiveOptimizerMemoryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::BlockedByUnsafeRuntimeFilter => "blocked_by_unsafe_runtime_filter",
            Self::BlockedByMemoryBoundary => "blocked_by_memory_boundary",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::ReportOnlyPlanned)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct AdaptiveOptimizerMemoryReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: AdaptiveOptimizerMemoryStatus,
    pub optimizer_phase: OptimizerPhase,
    pub rule_decisions: Vec<OptimizerRuleDecision>,
    pub runtime_filters: Vec<RuntimeFilter>,
    pub dynamic_pruning_decision: DynamicPruningDecision,
    pub adaptive_decisions: Vec<AdaptiveExecutionDecision>,
    pub skew_signals: Vec<SkewSignal>,
    pub conservative_runtime_filter_required: bool,
    pub dynamic_pruning_requires_proof: bool,
    pub memory_budget_required: bool,
    pub bounded_memory_required: bool,
    pub spill_policy_required: bool,
    pub deterministic_oom_boundary: bool,
    pub sink_requirement_boundary_required: bool,
    pub runtime_fact_required_before_adaptation: bool,
    pub adaptive_parallelism_required: bool,
    pub compaction_write_boundary_required: bool,
    pub optimizer_execution: bool,
    pub runtime_adaptation_applied: bool,
    pub runtime_filter_built: bool,
    pub runtime_filter_applied: bool,
    pub adaptive_parallelism_applied: bool,
    pub compaction_write_allowed: bool,
    pub compaction_execution_allowed: bool,
    pub plan_rewritten: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_engine_execution: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl AdaptiveOptimizerMemoryReport {
    #[must_use]
    pub fn cg14_foundation() -> Self {
        let rule_decisions = vec![
            OptimizerRuleDecision::deferred(
                static_rule_id("cg14.runtime_filter.conservative_gate"),
                OptimizerPhase::RuntimeAdaptive,
                OptimizerRuleKind::RuntimeFilterPushdown,
                "Runtime filters require conservative proof before they can be built or applied.",
            ),
            OptimizerRuleDecision::deferred(
                static_rule_id("cg14.dynamic_pruning.proof_gate"),
                OptimizerPhase::RuntimeAdaptive,
                OptimizerRuleKind::DynamicPruning,
                "Dynamic pruning requires runtime proof and deterministic diagnostics.",
            ),
            OptimizerRuleDecision::deferred(
                static_rule_id("cg14.memory_spill.boundary_gate"),
                OptimizerPhase::RuntimeAdaptive,
                OptimizerRuleKind::MemorySpillAwarePlanning,
                "Memory pressure can choose only bounded native adaptations until spill execution is implemented.",
            ),
        ];
        let runtime_filters = vec![RuntimeFilter::planned(
            RuntimeFilterKind::Range,
            "Conservative range runtime filter candidate; no filter is built or applied in report-only mode.",
        )];
        let adaptive_decisions = vec![
            AdaptiveExecutionDecision::new(
                AdaptiveTrigger::new(
                    AdaptiveTriggerKind::MemoryPressure,
                    "memory pressure fact would be required at runtime",
                ),
                AdaptiveDecisionKind::ReduceParallelism,
                "Candidate adaptation only; no plan rewrite or execution occurs.",
            ),
            AdaptiveExecutionDecision::new(
                AdaptiveTrigger::new(
                    AdaptiveTriggerKind::RuntimeFilterAvailable,
                    "runtime filter availability would require a conservative proof",
                ),
                AdaptiveDecisionKind::ApplyRuntimeFilter,
                "Candidate adaptation only; runtime filter application remains disabled.",
            ),
        ];
        let skew_signals = vec![SkewSignal::new(
            SkewSignalKind::SegmentRowCount,
            SkewSeverity::Unknown,
            "Skew detection is represented as a future runtime fact, not measured here.",
        )];

        Self {
            schema_version: ADAPTIVE_MEMORY_SCHEMA_VERSION,
            report_id: ADAPTIVE_MEMORY_REPORT_ID.to_string(),
            status: AdaptiveOptimizerMemoryStatus::ReportOnlyPlanned,
            optimizer_phase: OptimizerPhase::RuntimeAdaptive,
            rule_decisions,
            runtime_filters,
            dynamic_pruning_decision: DynamicPruningDecision::Candidate {
                reason: "Dynamic pruning is candidate-only until runtime proof exists.".to_string(),
            },
            adaptive_decisions,
            skew_signals,
            conservative_runtime_filter_required: true,
            dynamic_pruning_requires_proof: true,
            memory_budget_required: true,
            bounded_memory_required: true,
            spill_policy_required: true,
            deterministic_oom_boundary: true,
            sink_requirement_boundary_required: true,
            runtime_fact_required_before_adaptation: true,
            adaptive_parallelism_required: true,
            compaction_write_boundary_required: true,
            optimizer_execution: false,
            runtime_adaptation_applied: false,
            runtime_filter_built: false,
            runtime_filter_applied: false,
            adaptive_parallelism_applied: false,
            compaction_write_allowed: false,
            compaction_execution_allowed: false,
            plan_rewritten: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn rule_decision_count(&self) -> usize {
        self.rule_decisions.len()
    }

    #[must_use]
    pub fn runtime_filter_count(&self) -> usize {
        self.runtime_filters.len()
    }

    #[must_use]
    pub fn adaptive_decision_count(&self) -> usize {
        self.adaptive_decisions.len()
    }

    #[must_use]
    pub fn skew_signal_count(&self) -> usize {
        self.skew_signals.len()
    }

    #[must_use]
    pub fn deferred_rule_count(&self) -> usize {
        self.rule_decisions
            .iter()
            .filter(|decision| decision.status == OptimizerRuleStatus::Deferred)
            .count()
    }

    #[must_use]
    pub fn conservative_runtime_filter_count(&self) -> usize {
        self.runtime_filters
            .iter()
            .filter(|filter| filter.conservative)
            .count()
    }

    #[must_use]
    pub fn adaptive_runtime_gate_surface_order(&self) -> Vec<&'static str> {
        vec![
            "runtime_filter",
            "dynamic_pruning",
            "skew_signal",
            "adaptive_parallelism",
            "compaction_write",
        ]
    }

    #[must_use]
    pub fn runtime_gate_prerequisite_order(&self) -> Vec<&'static str> {
        vec![
            "conservative_runtime_filter_proof",
            "runtime_fact_evidence",
            "bounded_memory_budget",
            "spill_policy",
            "skew_signal_measurement",
            "adaptive_parallelism_policy",
            "compaction_plan_evidence",
            "write_intent",
            "execution_certificate",
            "native_io_certificate",
            "no_fallback_evidence",
        ]
    }

    #[must_use]
    pub fn runtime_gate_prerequisite_count(&self) -> usize {
        self.runtime_gate_prerequisite_order().len()
    }

    #[must_use]
    pub const fn support_status(&self) -> &'static str {
        "report_only"
    }

    #[must_use]
    pub const fn claim_gate_status(&self) -> &'static str {
        "not_claim_grade"
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.optimizer_execution
            && !self.runtime_adaptation_applied
            && !self.runtime_filter_built
            && !self.runtime_filter_applied
            && !self.adaptive_parallelism_applied
            && !self.compaction_write_allowed
            && !self.compaction_execution_allowed
            && !self.plan_rewritten
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .rule_decisions
                .iter()
                .any(OptimizerRuleDecision::has_errors)
            || self.runtime_filters.iter().any(RuntimeFilter::has_errors)
            || self
                .adaptive_decisions
                .iter()
                .any(AdaptiveExecutionDecision::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "adaptive optimizer memory plan\nschema_version: {}\nreport: {}\nstatus: {}\nphase: {}\nrules: {}\nruntime filters: {}\nadaptive decisions: {}\nside-effect-free: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.optimizer_phase.as_str(),
            self.rule_decision_count(),
            self.runtime_filter_count(),
            self.adaptive_decision_count(),
            self.is_side_effect_free(),
        )
    }
}

#[must_use]
pub fn plan_adaptive_optimizer_memory() -> AdaptiveOptimizerMemoryReport {
    AdaptiveOptimizerMemoryReport::cg14_foundation()
}

fn static_rule_id(value: &str) -> OptimizerRuleId {
    OptimizerRuleId::new(value).expect("static optimizer rule id is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn optimizer_rule_id_rejects_empty_ids() {
        assert!(OptimizerRuleId::new(" ").is_err());
    }
    #[test]
    fn optimizer_phase_label_stable() {
        assert_eq!(
            OptimizerPhase::VortexPhysical.canonical_label(),
            "vortex_physical"
        );
    }
    #[test]
    fn segment_pruning_is_vortex_native() {
        assert!(OptimizerRuleKind::SegmentPruning.is_vortex_native());
    }
    #[test]
    fn runtime_filter_pushdown_runtime_adaptive() {
        assert!(OptimizerRuleKind::RuntimeFilterPushdown.is_runtime_adaptive());
    }
    #[test]
    fn unsupported_rule_status_is_error() {
        assert!(OptimizerRuleStatus::Unsupported.is_error());
    }
    #[test]
    fn deferred_rule_status_is_not_error() {
        assert!(!OptimizerRuleStatus::Deferred.is_error());
    }
    #[test]
    fn rule_unsupported_has_fallback_false() {
        let id = OptimizerRuleId::new("r").unwrap();
        let d = OptimizerRuleDecision::unsupported(
            id,
            OptimizerPhase::Logical,
            OptimizerRuleKind::Unsupported,
            "x",
            "y",
        );
        assert!(d.has_errors());
        assert!(!d.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn cost_value_behavior() {
        assert!(CostValue::known(1).is_known_or_estimated());
        assert!(CostValue::estimated(2).is_known_or_estimated());
        assert!(!CostValue::unknown().is_known_or_estimated());
    }
    #[test]
    fn cost_model_unknown_projection_none() {
        assert_eq!(CostModelInput::unknown().projection_ratio(), None);
    }
    #[test]
    fn dict_filter_conservative() {
        assert!(RuntimeFilterKind::DictionaryId.is_conservative_candidate());
    }
    #[test]
    fn runtime_filter_unsupported_has_errors() {
        let f = RuntimeFilter::unsupported(RuntimeFilterKind::Unsupported, "x", "y");
        assert!(f.has_errors());
        assert!(!f.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn dynamic_pruning_rejected_error() {
        assert!(DynamicPruningDecision::Rejected { reason: "x".into() }.is_error());
    }
    #[test]
    fn adaptive_nochange_no_plan_change() {
        assert!(!AdaptiveDecisionKind::NoChange.changes_plan());
    }
    #[test]
    fn adaptive_split_changes_plan() {
        assert!(AdaptiveDecisionKind::SplitTasks.changes_plan());
    }
    #[test]
    fn adaptive_unsupported_has_errors() {
        let d = AdaptiveExecutionDecision::unsupported(
            AdaptiveTrigger::new(AdaptiveTriggerKind::Unknown, "x"),
            "f",
            "r",
        );
        assert!(d.has_errors());
    }
    #[test]
    fn broadcast_no_shuffle() {
        assert!(!JoinStrategy::BroadcastSmallSide.requires_shuffle());
    }
    #[test]
    fn dictionary_join_encoded_friendly() {
        assert!(JoinStrategy::DictionaryAwareJoin.is_encoded_friendly());
    }
    #[test]
    fn metadata_only_agg_vortex_native() {
        assert!(AggregateStrategy::MetadataOnly.is_vortex_native());
    }
    #[test]
    fn hash_agg_may_spill() {
        assert!(AggregateStrategy::HashAggregate.may_require_spill());
    }
    #[test]
    fn high_skew_requires_action() {
        assert!(SkewSeverity::High.requires_action());
    }
    #[test]
    fn dynamic_repartition_requires_repartition() {
        assert!(SkewHandlingStrategy::DynamicRepartition.requires_repartition());
    }
    #[test]
    fn skeleton_not_implemented_has_errors() {
        assert!(
            OptimizerPlanSkeleton::not_implemented(OptimizerPhase::Logical, "x", "y").has_errors()
        );
    }
    #[test]
    fn skeleton_human_mentions_fallback_disabled() {
        assert!(
            OptimizerPlanSkeleton::new(OptimizerPhase::Logical)
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn adaptive_optimizer_memory_foundation_is_report_only() {
        let report = AdaptiveOptimizerMemoryReport::cg14_foundation();
        assert_eq!(
            report.status,
            AdaptiveOptimizerMemoryStatus::ReportOnlyPlanned
        );
        assert_eq!(report.rule_decision_count(), 3);
        assert_eq!(report.deferred_rule_count(), 3);
        assert_eq!(report.runtime_filter_count(), 1);
        assert_eq!(report.conservative_runtime_filter_count(), 1);
        assert_eq!(report.adaptive_decision_count(), 2);
        assert_eq!(report.skew_signal_count(), 1);
        assert_eq!(
            report.adaptive_runtime_gate_surface_order(),
            vec![
                "runtime_filter",
                "dynamic_pruning",
                "skew_signal",
                "adaptive_parallelism",
                "compaction_write"
            ]
        );
        assert_eq!(report.runtime_gate_prerequisite_count(), 11);
        assert_eq!(report.support_status(), "report_only");
        assert_eq!(report.claim_gate_status(), "not_claim_grade");
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.runtime_adaptation_applied);
        assert!(!report.runtime_filter_built);
        assert!(!report.runtime_filter_applied);
        assert!(!report.adaptive_parallelism_applied);
        assert!(!report.compaction_write_allowed);
        assert!(!report.compaction_execution_allowed);
        assert!(!report.plan_rewritten);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }

    #[test]
    fn evidence_aware_optimizer_trace_is_report_only_and_side_effect_free() {
        let report = plan_evidence_aware_optimizer_trace();
        assert_eq!(
            report.schema_version,
            EVIDENCE_AWARE_OPTIMIZER_SCHEMA_VERSION
        );
        assert_eq!(report.optimizer_phase, OptimizerPhase::Logical);
        assert_eq!(report.support_status, "report_only");
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.rule_count(), 9);
        assert_eq!(report.applied_rule_count(), 0);
        assert_eq!(report.admitted_rule_count(), 1);
        assert_eq!(report.blocked_rule_count(), 3);
        assert_eq!(report.unsupported_rule_count(), 2);
        assert_eq!(report.not_applicable_rule_count(), 1);
        assert_eq!(report.report_only_rule_count(), 2);
        assert!(report.all_no_fallback_no_external_engine());
        assert!(!report.optimizer_execution);
        assert!(!report.plan_rewritten);
        assert!(!report.performance_claim_allowed);
        assert!(!report.broad_sql_dataframe_claim_allowed);
    }

    #[test]
    fn evidence_aware_optimizer_trace_preserves_rule_evidence_boundaries() {
        let report = plan_evidence_aware_optimizer_trace();
        let source_state = report
            .rule_rows
            .iter()
            .find(|row| row.rule_id == "common_subplan_source_state_reuse")
            .expect("source-state reuse row");
        assert_eq!(source_state.rule_status, OptimizerTraceRuleStatus::Admitted);
        assert!(source_state.source_state_reuse_admitted);
        assert!(!source_state.rule_status.applied());
        assert_eq!(source_state.before_plan_digest, REPORT_ONLY_PLAN_DIGEST);
        assert_eq!(source_state.after_plan_digest, REPORT_ONLY_PLAN_DIGEST);

        assert!(report.rule_rows.iter().all(|row| {
            row.evidence_preserved
                && row.no_fallback_preserved
                && row.claim_boundary_preserved
                && row.materialization_boundary_preserved
                && !row.fallback_attempted
                && !row.external_engine_invoked
                && row.claim_gate_status == "not_claim_grade"
        }));
    }
}
