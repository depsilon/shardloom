//! Capability certification contracts for user-facing coverage surfaces.
//!
//! This module defines report-only CG-20 contracts. It intentionally does not
//! parse SQL, execute operators, register functions, probe adapters, or perform
//! any filesystem/network I/O.

use std::fmt::Write as _;

use crate::{Diagnostic, DiagnosticSeverity, Result, ShardLoomError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityCertificationSurface {
    Sql,
    Operator,
    Function,
    Adapter,
    SemanticProfile,
    Migration,
    Scorecard,
}

impl CapabilityCertificationSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Operator => "operator",
            Self::Function => "function",
            Self::Adapter => "adapter",
            Self::SemanticProfile => "semantic_profile",
            Self::Migration => "migration",
            Self::Scorecard => "scorecard",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityCertificationStatus {
    Unsupported,
    Planned,
    Partial,
    TestReferenceOnly,
    Native,
    Certified,
    Blocked,
}

impl CapabilityCertificationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Planned => "planned",
            Self::Partial => "partial",
            Self::TestReferenceOnly => "test_reference_only",
            Self::Native => "native",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }

    #[must_use]
    pub const fn is_reference_only(&self) -> bool {
        matches!(self, Self::TestReferenceOnly)
    }

    #[must_use]
    pub const fn has_native_capability(&self) -> bool {
        matches!(self, Self::Native | Self::Certified)
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        matches!(self, Self::Certified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCertificationEntry {
    pub name: String,
    pub surface: CapabilityCertificationSurface,
    pub status: CapabilityCertificationStatus,
    pub evidence_notes: Vec<String>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl CapabilityCertificationEntry {
    /// Creates a certification entry with no fallback attempted.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty.
    pub fn new(
        name: impl Into<String>,
        surface: CapabilityCertificationSurface,
        status: CapabilityCertificationStatus,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "capability certification entry name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
            surface,
            status,
            evidence_notes: Vec::new(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        })
    }

    /// Creates a planned entry.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty.
    pub fn planned(
        name: impl Into<String>,
        surface: CapabilityCertificationSurface,
    ) -> Result<Self> {
        Self::new(name, surface, CapabilityCertificationStatus::Planned)
    }

    #[must_use]
    pub fn with_evidence_note(mut self, note: impl Into<String>) -> Self {
        self.evidence_notes.push(note.into());
        self
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback_attempted
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}:{}:{}",
            self.surface.as_str(),
            self.name,
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticProfileName {
    ShardLoomNative,
    SparkCompatible,
    DataFusionCompatible,
    PostgresLike,
    AnsiStrict,
}

impl SemanticProfileName {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ShardLoomNative => "shardloom_native",
            Self::SparkCompatible => "spark_compatible",
            Self::DataFusionCompatible => "datafusion_compatible",
            Self::PostgresLike => "postgres_like",
            Self::AnsiStrict => "ansi_strict",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::ShardLoomNative,
            Self::SparkCompatible,
            Self::DataFusionCompatible,
            Self::PostgresLike,
            Self::AnsiStrict,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCoverageTier {
    Unsupported,
    ParsedOnly,
    BoundValidated,
    NativeLogicalPlan,
    NativePhysicalPlan,
    NativeDecodedOrTestReference,
    EncodedCapableNative,
    BenchmarkedCertified,
}

impl SqlCoverageTier {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unsupported => "s0_unsupported",
            Self::ParsedOnly => "s1_parsed_only",
            Self::BoundValidated => "s2_bound_validated",
            Self::NativeLogicalPlan => "s3_native_logical_plan",
            Self::NativePhysicalPlan => "s4_native_physical_plan",
            Self::NativeDecodedOrTestReference => "s5_native_decoded_or_test_reference",
            Self::EncodedCapableNative => "s6_encoded_capable_native",
            Self::BenchmarkedCertified => "s7_benchmarked_certified",
        }
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        matches!(self, Self::BenchmarkedCertified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlFeatureGroup {
    Select,
    WithCte,
    FromTableOrSubquery,
    Where,
    ProjectionAliases,
    GroupBy,
    Having,
    OrderBy,
    LimitOffset,
    Distinct,
    CaseWhen,
    Casts,
    ScalarFunctions,
    AggregateFunctions,
    WindowFunctions,
    Subqueries,
    Joins,
    SetOperations,
    CreateTableAsSelect,
    Insert,
    MergeUpdateDelete,
    Explain,
    AnalyzeProfile,
}

impl SqlFeatureGroup {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Select => "select",
            Self::WithCte => "with_cte",
            Self::FromTableOrSubquery => "from_table_or_subquery",
            Self::Where => "where",
            Self::ProjectionAliases => "projection_aliases",
            Self::GroupBy => "group_by",
            Self::Having => "having",
            Self::OrderBy => "order_by",
            Self::LimitOffset => "limit_offset",
            Self::Distinct => "distinct",
            Self::CaseWhen => "case_when",
            Self::Casts => "casts",
            Self::ScalarFunctions => "scalar_functions",
            Self::AggregateFunctions => "aggregate_functions",
            Self::WindowFunctions => "window_functions",
            Self::Subqueries => "subqueries",
            Self::Joins => "joins",
            Self::SetOperations => "set_operations",
            Self::CreateTableAsSelect => "create_table_as_select",
            Self::Insert => "insert",
            Self::MergeUpdateDelete => "merge_update_delete",
            Self::Explain => "explain",
            Self::AnalyzeProfile => "analyze_profile",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Select,
            Self::WithCte,
            Self::FromTableOrSubquery,
            Self::Where,
            Self::ProjectionAliases,
            Self::GroupBy,
            Self::Having,
            Self::OrderBy,
            Self::LimitOffset,
            Self::Distinct,
            Self::CaseWhen,
            Self::Casts,
            Self::ScalarFunctions,
            Self::AggregateFunctions,
            Self::WindowFunctions,
            Self::Subqueries,
            Self::Joins,
            Self::SetOperations,
            Self::CreateTableAsSelect,
            Self::Insert,
            Self::MergeUpdateDelete,
            Self::Explain,
            Self::AnalyzeProfile,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCoverageEntry {
    pub feature: SqlFeatureGroup,
    pub tier: SqlCoverageTier,
    pub status: CapabilityCertificationStatus,
    pub semantic_profile: SemanticProfileName,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl SqlCoverageEntry {
    #[must_use]
    pub const fn planned(feature: SqlFeatureGroup) -> Self {
        Self {
            feature,
            tier: SqlCoverageTier::Unsupported,
            status: CapabilityCertificationStatus::Planned,
            semantic_profile: SemanticProfileName::ShardLoomNative,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        self.status.can_satisfy_production_claim()
            && self.tier.can_satisfy_production_claim()
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCoverageMatrix {
    pub schema_version: &'static str,
    pub entries: Vec<SqlCoverageEntry>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl SqlCoverageMatrix {
    #[must_use]
    pub fn planned_foundation() -> Self {
        Self {
            schema_version: "shardloom.sql_coverage.v1",
            entries: SqlFeatureGroup::all()
                .iter()
                .copied()
                .map(SqlCoverageEntry::planned)
                .collect(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDataFramePlannerReadinessSurface {
    SqlTextAdmission,
    SqlParse,
    SqlBind,
    SqlPlan,
    SqlExecute,
    DataFrameLazyPlan,
    DataFrameExpressionBuilder,
    DataFrameJoin,
    DataFrameAggregate,
    DataFrameWindow,
    PlanDiagnostics,
    UnsupportedExecutionState,
}

impl SqlDataFramePlannerReadinessSurface {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SqlTextAdmission => "sql_text_admission",
            Self::SqlParse => "sql_parse",
            Self::SqlBind => "sql_bind",
            Self::SqlPlan => "sql_plan",
            Self::SqlExecute => "sql_execute",
            Self::DataFrameLazyPlan => "dataframe_lazy_plan",
            Self::DataFrameExpressionBuilder => "dataframe_expression_builder",
            Self::DataFrameJoin => "dataframe_join",
            Self::DataFrameAggregate => "dataframe_aggregate",
            Self::DataFrameWindow => "dataframe_window",
            Self::PlanDiagnostics => "plan_diagnostics",
            Self::UnsupportedExecutionState => "unsupported_execution_state",
        }
    }

    #[must_use]
    pub const fn is_sql(self) -> bool {
        matches!(
            self,
            Self::SqlTextAdmission
                | Self::SqlParse
                | Self::SqlBind
                | Self::SqlPlan
                | Self::SqlExecute
        )
    }

    #[must_use]
    pub const fn is_dataframe(self) -> bool {
        matches!(
            self,
            Self::DataFrameLazyPlan
                | Self::DataFrameExpressionBuilder
                | Self::DataFrameJoin
                | Self::DataFrameAggregate
                | Self::DataFrameWindow
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerReadinessSupportStatus {
    ReportOnly,
    Unsupported,
}

impl PlannerReadinessSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SqlDataFramePlannerReadinessRow {
    pub row_id: &'static str,
    pub surface: SqlDataFramePlannerReadinessSurface,
    pub support_status: PlannerReadinessSupportStatus,
    pub claim_gate_status: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_evidence: &'static str,
    pub user_visible_surface: &'static str,
    pub parser_executed: bool,
    pub binder_executed: bool,
    pub planner_executed: bool,
    pub runtime_execution: bool,
    pub dataframe_runtime: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl SqlDataFramePlannerReadinessRow {
    #[must_use]
    pub const fn sql_text_admission() -> Self {
        Self {
            row_id: "sql_text_admission",
            surface: SqlDataFramePlannerReadinessSurface::SqlTextAdmission,
            support_status: PlannerReadinessSupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_SQL_TEXT_ADMISSION_REPORT_ONLY",
            blocker_id: "gar0001a.sql_text_admission_report_only",
            required_evidence: "sql_parser,sql_ast_contract,unsupported_diagnostic_snapshot",
            user_visible_surface: "capabilities sql,workflow-unsupported-plan sql",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn sql_parse() -> Self {
        Self {
            row_id: "sql_parse",
            surface: SqlDataFramePlannerReadinessSurface::SqlParse,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.sql.parse_unsupported",
            required_evidence: "sql_parser,sql_ast_contract,unsupported_diagnostic_snapshot",
            user_visible_surface: "workflow-unsupported-plan sql-parse",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn sql_bind() -> Self {
        Self {
            row_id: "sql_bind",
            surface: SqlDataFramePlannerReadinessSurface::SqlBind,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.sql.bind_unsupported",
            required_evidence: "sql_binder,catalog_schema_contract,name_resolution_policy,semantic_conformance_suite",
            user_visible_surface: "workflow-unsupported-plan sql-bind",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn sql_plan() -> Self {
        Self {
            row_id: "sql_plan",
            surface: SqlDataFramePlannerReadinessSurface::SqlPlan,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.sql.plan_unsupported",
            required_evidence: "sql_logical_plan_lowering,operator_capability_matrix,semantic_conformance_suite",
            user_visible_surface: "workflow-unsupported-plan sql-plan",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn sql_execute() -> Self {
        Self {
            row_id: "sql_execute",
            surface: SqlDataFramePlannerReadinessSurface::SqlExecute,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.sql.execute_unsupported",
            required_evidence: "sql_parser,binder,planner,semantic_conformance_suite,execution_certificate,native_io_certificate",
            user_visible_surface: "workflow-unsupported-plan sql-execute",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dataframe_lazy_plan() -> Self {
        Self {
            row_id: "dataframe_lazy_plan",
            surface: SqlDataFramePlannerReadinessSurface::DataFrameLazyPlan,
            support_status: PlannerReadinessSupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_DATAFRAME_LAZY_PLAN_REPORT_ONLY",
            blocker_id: "gar0001a.dataframe_lazy_plan_report_only",
            required_evidence: "typed_lazy_plan_contract,capability_snapshot,unsupported_diagnostic_snapshot",
            user_visible_surface: "python LazyFrame,capabilities dataframe",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dataframe_expression_builder() -> Self {
        Self {
            row_id: "dataframe_expression_builder",
            surface: SqlDataFramePlannerReadinessSurface::DataFrameExpressionBuilder,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.with_column.expression_unsupported",
            required_evidence: "expression_ast_contract,type_inference,operator_capability_matrix,semantic_conformance_suite",
            user_visible_surface: "python LazyFrame.with_column",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dataframe_join() -> Self {
        Self {
            row_id: "dataframe_join",
            surface: SqlDataFramePlannerReadinessSurface::DataFrameJoin,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.join.operator_unsupported",
            required_evidence: "join_operator_capability,memory_spill_declaration,correctness_fixture,execution_certificate",
            user_visible_surface: "python LazyFrame.join",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dataframe_aggregate() -> Self {
        Self {
            row_id: "dataframe_aggregate",
            surface: SqlDataFramePlannerReadinessSurface::DataFrameAggregate,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.dataframe_aggregation_unsupported",
            required_evidence: "aggregate_operator_capability,memory_spill_declaration,correctness_fixture,execution_certificate",
            user_visible_surface: "python LazyFrame.agg,python GroupedLazyFrame.agg",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dataframe_window() -> Self {
        Self {
            row_id: "dataframe_window",
            surface: SqlDataFramePlannerReadinessSurface::DataFrameWindow,
            support_status: PlannerReadinessSupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
            blocker_id: "cg21.workflow.dataframe_window_unsupported",
            required_evidence: "window_operator_capability,sort_capability,correctness_fixture,execution_certificate",
            user_visible_surface: "python LazyFrame.window",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn plan_diagnostics() -> Self {
        Self {
            row_id: "plan_diagnostics",
            surface: SqlDataFramePlannerReadinessSurface::PlanDiagnostics,
            support_status: PlannerReadinessSupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_PLANNER_READINESS_DIAGNOSTICS_REPORT_ONLY",
            blocker_id: "gar0001a.plan_diagnostics_report_only",
            required_evidence: "stable_diagnostic_codes,capability_report_refs,unsupported_snapshot_tests",
            user_visible_surface: "capabilities sql,capabilities dataframe,workflow-unsupported-plan",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn unsupported_execution_state() -> Self {
        Self {
            row_id: "unsupported_execution_state",
            surface: SqlDataFramePlannerReadinessSurface::UnsupportedExecutionState,
            support_status: PlannerReadinessSupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_PLANNER_EXECUTION_STATE",
            blocker_id: "gar0001a.unsupported_execution_state",
            required_evidence: "execution_certificate,native_io_certificate,semantic_conformance_suite,benchmark_row",
            user_visible_surface: "capability discovery,Python capability view",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }

    #[must_use]
    pub fn not_claim_grade(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub const fn non_executing(&self) -> bool {
        !self.parser_executed
            && !self.binder_executed
            && !self.planner_executed
            && !self.runtime_execution
            && !self.dataframe_runtime
    }

    #[must_use]
    pub fn deterministic_diagnostic_present(&self) -> bool {
        !self.unsupported_diagnostic_code.is_empty()
            && self.unsupported_diagnostic_code != "none"
            && !self.blocker_id.is_empty()
            && self.blocker_id != "none"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SqlDataFramePlannerReadinessMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub rows: Vec<SqlDataFramePlannerReadinessRow>,
    pub claim_gate_status: &'static str,
    pub support_status_vocabulary: &'static str,
    pub report_ref: &'static str,
    pub docs_ref: &'static str,
    pub parser_executed: bool,
    pub binder_executed: bool,
    pub planner_executed: bool,
    pub runtime_execution: bool,
    pub dataframe_runtime: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl SqlDataFramePlannerReadinessMatrix {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.sql_dataframe_planner_readiness.v1",
            matrix_id: "gar0001a.sql_dataframe_planner_readiness",
            rows: vec![
                SqlDataFramePlannerReadinessRow::sql_text_admission(),
                SqlDataFramePlannerReadinessRow::sql_parse(),
                SqlDataFramePlannerReadinessRow::sql_bind(),
                SqlDataFramePlannerReadinessRow::sql_plan(),
                SqlDataFramePlannerReadinessRow::sql_execute(),
                SqlDataFramePlannerReadinessRow::dataframe_lazy_plan(),
                SqlDataFramePlannerReadinessRow::dataframe_expression_builder(),
                SqlDataFramePlannerReadinessRow::dataframe_join(),
                SqlDataFramePlannerReadinessRow::dataframe_aggregate(),
                SqlDataFramePlannerReadinessRow::dataframe_window(),
                SqlDataFramePlannerReadinessRow::plan_diagnostics(),
                SqlDataFramePlannerReadinessRow::unsupported_execution_state(),
            ],
            claim_gate_status: "not_claim_grade",
            support_status_vocabulary: "report_only,unsupported",
            report_ref: "capabilities://sql-dataframe-planner-readiness.v1",
            docs_ref: "docs/architecture/global-architecture-review.md#rfc-0001",
            parser_executed: false,
            binder_executed: false,
            planner_executed: false,
            runtime_execution: false,
            dataframe_runtime: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn sql_row_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.surface.is_sql())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn dataframe_row_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.surface.is_dataframe())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_codes(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.unsupported_diagnostic_code)
            .collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.blocker_id).collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .all(SqlDataFramePlannerReadinessRow::fallback_free)
    }

    #[must_use]
    pub fn all_rows_not_claim_grade(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
            && self
                .rows
                .iter()
                .all(SqlDataFramePlannerReadinessRow::not_claim_grade)
    }

    #[must_use]
    pub fn all_rows_non_executing(&self) -> bool {
        !self.parser_executed
            && !self.binder_executed
            && !self.planner_executed
            && !self.runtime_execution
            && !self.dataframe_runtime
            && self
                .rows
                .iter()
                .all(SqlDataFramePlannerReadinessRow::non_executing)
    }

    #[must_use]
    pub fn deterministic_diagnostics_present(&self) -> bool {
        self.rows
            .iter()
            .all(SqlDataFramePlannerReadinessRow::deterministic_diagnostic_present)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorFamily {
    Scan,
    Filter,
    Project,
    Limit,
    TopK,
    Sort,
    Aggregate,
    HashAggregate,
    SortAggregate,
    Window,
    Join,
    HashJoin,
    SortMergeJoin,
    BroadcastJoin,
    SemiJoin,
    AntiJoin,
    RangeJoin,
    SetUnion,
    SetIntersect,
    SetExcept,
    Repartition,
    ShuffleExchange,
    Write,
    Commit,
    Compact,
    Merge,
    Delete,
}

impl OperatorFamily {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Project => "project",
            Self::Limit => "limit",
            Self::TopK => "top_k",
            Self::Sort => "sort",
            Self::Aggregate => "aggregate",
            Self::HashAggregate => "hash_aggregate",
            Self::SortAggregate => "sort_aggregate",
            Self::Window => "window",
            Self::Join => "join",
            Self::HashJoin => "hash_join",
            Self::SortMergeJoin => "sort_merge_join",
            Self::BroadcastJoin => "broadcast_join",
            Self::SemiJoin => "semi_join",
            Self::AntiJoin => "anti_join",
            Self::RangeJoin => "range_join",
            Self::SetUnion => "set_union",
            Self::SetIntersect => "set_intersect",
            Self::SetExcept => "set_except",
            Self::Repartition => "repartition",
            Self::ShuffleExchange => "shuffle_exchange",
            Self::Write => "write",
            Self::Commit => "commit",
            Self::Compact => "compact",
            Self::Merge => "merge",
            Self::Delete => "delete",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Scan,
            Self::Filter,
            Self::Project,
            Self::Limit,
            Self::TopK,
            Self::Sort,
            Self::Aggregate,
            Self::HashAggregate,
            Self::SortAggregate,
            Self::Window,
            Self::Join,
            Self::HashJoin,
            Self::SortMergeJoin,
            Self::BroadcastJoin,
            Self::SemiJoin,
            Self::AntiJoin,
            Self::RangeJoin,
            Self::SetUnion,
            Self::SetIntersect,
            Self::SetExcept,
            Self::Repartition,
            Self::ShuffleExchange,
            Self::Write,
            Self::Commit,
            Self::Compact,
            Self::Merge,
            Self::Delete,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCertificationStatus {
    Unsupported,
    Planned,
    Parsed,
    PlannedNative,
    TestReferenceOnly,
    NativeDecoded,
    EncodedCapable,
    CompressedNative,
    StreamingCapable,
    SpillCapable,
    DistributedCapable,
    Benchmarked,
    ProductionCertified,
}

impl OperatorCertificationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Planned => "planned",
            Self::Parsed => "parsed",
            Self::PlannedNative => "planned_native",
            Self::TestReferenceOnly => "test_reference_only",
            Self::NativeDecoded => "native_decoded",
            Self::EncodedCapable => "encoded_capable",
            Self::CompressedNative => "compressed_native",
            Self::StreamingCapable => "streaming_capable",
            Self::SpillCapable => "spill_capable",
            Self::DistributedCapable => "distributed_capable",
            Self::Benchmarked => "benchmarked",
            Self::ProductionCertified => "production_certified",
        }
    }

    #[must_use]
    pub const fn is_reference_only(&self) -> bool {
        matches!(self, Self::TestReferenceOnly)
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        matches!(self, Self::ProductionCertified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct OperatorMemoryCertification {
    pub streaming: bool,
    pub bounded_memory: bool,
    pub spillable: bool,
    pub requires_full_materialization: bool,
    pub requires_shuffle: bool,
    pub oom_safe: bool,
}

impl OperatorMemoryCertification {
    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            streaming: false,
            bounded_memory: false,
            spillable: false,
            requires_full_materialization: false,
            requires_shuffle: false,
            oom_safe: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorCoverageEntry {
    pub family: OperatorFamily,
    pub status: OperatorCertificationStatus,
    pub memory: OperatorMemoryCertification,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl OperatorCoverageEntry {
    #[must_use]
    pub const fn planned(family: OperatorFamily) -> Self {
        Self {
            family,
            status: OperatorCertificationStatus::Planned,
            memory: OperatorMemoryCertification::unsupported(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorCoverageMatrix {
    pub schema_version: &'static str,
    pub entries: Vec<OperatorCoverageEntry>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl OperatorCoverageMatrix {
    #[must_use]
    pub fn planned_foundation() -> Self {
        Self {
            schema_version: "shardloom.operator_coverage.v1",
            entries: OperatorFamily::all()
                .iter()
                .copied()
                .map(OperatorCoverageEntry::planned)
                .collect(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCoverageGroup {
    Comparison,
    Boolean,
    Math,
    Numeric,
    Decimal,
    String,
    Regex,
    Binary,
    Date,
    Time,
    Timestamp,
    Interval,
    Timezone,
    Conditional,
    NullHandling,
    Casts,
    Hashing,
    EncodingAwarePredicates,
    Aggregates,
    ApproximateAggregates,
    StatisticalAggregates,
    WindowFunctions,
    ArrayListFunctions,
    StructFunctions,
    MapFunctions,
    JsonFunctions,
    UuidIdFunctions,
    TableFunctions,
    MetadataFunctions,
    SystemIntrospectionFunctions,
    VectorFunctions,
    EffectfulFunctions,
}

impl FunctionCoverageGroup {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Comparison => "comparison",
            Self::Boolean => "boolean",
            Self::Math => "math",
            Self::Numeric => "numeric",
            Self::Decimal => "decimal",
            Self::String => "string",
            Self::Regex => "regex",
            Self::Binary => "binary",
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::Interval => "interval",
            Self::Timezone => "timezone",
            Self::Conditional => "conditional",
            Self::NullHandling => "null_handling",
            Self::Casts => "casts",
            Self::Hashing => "hashing",
            Self::EncodingAwarePredicates => "encoding_aware_predicates",
            Self::Aggregates => "aggregates",
            Self::ApproximateAggregates => "approximate_aggregates",
            Self::StatisticalAggregates => "statistical_aggregates",
            Self::WindowFunctions => "window_functions",
            Self::ArrayListFunctions => "array_list_functions",
            Self::StructFunctions => "struct_functions",
            Self::MapFunctions => "map_functions",
            Self::JsonFunctions => "json_functions",
            Self::UuidIdFunctions => "uuid_id_functions",
            Self::TableFunctions => "table_functions",
            Self::MetadataFunctions => "metadata_functions",
            Self::SystemIntrospectionFunctions => "system_introspection_functions",
            Self::VectorFunctions => "vector_functions",
            Self::EffectfulFunctions => "effectful_functions",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Comparison,
            Self::Boolean,
            Self::Math,
            Self::Numeric,
            Self::Decimal,
            Self::String,
            Self::Regex,
            Self::Binary,
            Self::Date,
            Self::Time,
            Self::Timestamp,
            Self::Interval,
            Self::Timezone,
            Self::Conditional,
            Self::NullHandling,
            Self::Casts,
            Self::Hashing,
            Self::EncodingAwarePredicates,
            Self::Aggregates,
            Self::ApproximateAggregates,
            Self::StatisticalAggregates,
            Self::WindowFunctions,
            Self::ArrayListFunctions,
            Self::StructFunctions,
            Self::MapFunctions,
            Self::JsonFunctions,
            Self::UuidIdFunctions,
            Self::TableFunctions,
            Self::MetadataFunctions,
            Self::SystemIntrospectionFunctions,
            Self::VectorFunctions,
            Self::EffectfulFunctions,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct FunctionCoverageEntry {
    pub group: FunctionCoverageGroup,
    pub status: CapabilityCertificationStatus,
    pub encoded_capable: bool,
    pub selection_vector_supported: bool,
    pub streaming_supported: bool,
    pub spill_supported: bool,
    pub materialization_required: bool,
    pub semantic_profile: SemanticProfileName,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl FunctionCoverageEntry {
    #[must_use]
    pub const fn planned(group: FunctionCoverageGroup) -> Self {
        Self {
            group,
            status: CapabilityCertificationStatus::Planned,
            encoded_capable: false,
            selection_vector_supported: false,
            streaming_supported: false,
            spill_supported: false,
            materialization_required: false,
            semantic_profile: SemanticProfileName::ShardLoomNative,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCoverageMatrix {
    pub schema_version: &'static str,
    pub entries: Vec<FunctionCoverageEntry>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl FunctionCoverageMatrix {
    #[must_use]
    pub fn planned_foundation() -> Self {
        Self {
            schema_version: "shardloom.function_coverage.v1",
            entries: FunctionCoverageGroup::all()
                .iter()
                .copied()
                .map(FunctionCoverageEntry::planned)
                .collect(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterMaturityLevel {
    DeclaredOnly,
    CapabilityDiscovery,
    SchemaMetadataDiscovery,
    ReadSupport,
    PushdownSupport,
    WriteSupport,
    CommitRecoverySupport,
    BenchmarkedCertified,
}

impl AdapterMaturityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DeclaredOnly => "a0_declared_only",
            Self::CapabilityDiscovery => "a1_capability_discovery",
            Self::SchemaMetadataDiscovery => "a2_schema_metadata_discovery",
            Self::ReadSupport => "a3_read_support",
            Self::PushdownSupport => "a4_pushdown_support",
            Self::WriteSupport => "a5_write_support",
            Self::CommitRecoverySupport => "a6_commit_recovery_support",
            Self::BenchmarkedCertified => "a7_benchmarked_certified",
        }
    }

    #[must_use]
    pub const fn can_read(&self) -> bool {
        matches!(
            self,
            Self::ReadSupport
                | Self::PushdownSupport
                | Self::WriteSupport
                | Self::CommitRecoverySupport
                | Self::BenchmarkedCertified
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePushdownExactness {
    Exact,
    ExactWithResidual,
    ConservativeMayIncludeFalsePositives,
    Unsupported,
    UnsafeRejected,
}

impl SourcePushdownExactness {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::ExactWithResidual => "exact_with_residual",
            Self::ConservativeMayIncludeFalsePositives => {
                "conservative_may_include_false_positives"
            }
            Self::Unsupported => "unsupported",
            Self::UnsafeRejected => "unsafe_rejected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct AdapterCertificationEntry {
    pub adapter_id: String,
    pub maturity: AdapterMaturityLevel,
    pub status: CapabilityCertificationStatus,
    pub source_kind: String,
    pub sink_kind: Option<String>,
    pub pushdown_exactness: SourcePushdownExactness,
    pub encoded_representation_preserved: bool,
    pub materialization_required: bool,
    pub read_supported: bool,
    pub write_supported: bool,
    pub commit_supported: bool,
    pub streaming_supported: bool,
    pub object_store_range_supported: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl AdapterCertificationEntry {
    /// Creates a planned adapter certification entry.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `adapter_id` is empty.
    pub fn planned(adapter_id: impl Into<String>, source_kind: impl Into<String>) -> Result<Self> {
        let adapter_id = adapter_id.into();
        if adapter_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "adapter certification id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            adapter_id,
            maturity: AdapterMaturityLevel::DeclaredOnly,
            status: CapabilityCertificationStatus::Planned,
            source_kind: source_kind.into(),
            sink_kind: None,
            pushdown_exactness: SourcePushdownExactness::Unsupported,
            encoded_representation_preserved: false,
            materialization_required: false,
            read_supported: false,
            write_supported: false,
            commit_supported: false,
            streaming_supported: false,
            object_store_range_supported: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterCertificationMatrix {
    pub schema_version: &'static str,
    pub entries: Vec<AdapterCertificationEntry>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl AdapterCertificationMatrix {
    #[must_use]
    pub fn planned_foundation() -> Self {
        let entries = [
            ("native_vortex", "native_vortex"),
            ("parquet", "parquet"),
            ("arrow_ipc", "arrow_ipc"),
            ("csv", "csv"),
            ("jsonl", "jsonl"),
            ("avro", "avro"),
            ("orc", "orc"),
            ("iceberg_compatible", "iceberg_compatible"),
            ("delta_compatible", "delta_compatible"),
            ("hive_partition_discovery", "hive_partition_discovery"),
            (
                "table_snapshot_import_export",
                "table_snapshot_import_export",
            ),
            ("schema_evolution_adapter", "schema_evolution_adapter"),
            ("local_filesystem", "local_filesystem"),
            ("s3_compatible", "s3_compatible"),
            ("gcs", "gcs"),
            ("azure_blob_adls", "azure_blob_adls"),
            ("http_range", "http_range"),
            ("local_catalog", "local_catalog"),
            ("hive_compatible_catalog", "hive_compatible_catalog"),
            (
                "iceberg_rest_compatible_catalog",
                "iceberg_rest_compatible_catalog",
            ),
            ("glue_like_catalog", "glue_like_catalog"),
            ("nessie_like_catalog", "nessie_like_catalog"),
            ("python_api", "python_api"),
            ("rust_api", "rust_api"),
        ]
        .into_iter()
        .filter_map(|(adapter_id, source_kind)| {
            AdapterCertificationEntry::planned(adapter_id, source_kind).ok()
        })
        .collect();

        Self {
            schema_version: "shardloom.adapter_certification.v1",
            entries,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticProfileEntry {
    pub profile: SemanticProfileName,
    pub status: CapabilityCertificationStatus,
    pub dimensions_declared: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl SemanticProfileEntry {
    #[must_use]
    pub const fn planned(profile: SemanticProfileName) -> Self {
        Self {
            profile,
            status: CapabilityCertificationStatus::Planned,
            dimensions_declared: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationReportKind {
    SparkMigration,
    DataFusionMigration,
    DuckDbPolarsMigration,
    SqlCompatibility,
    PlanPortability,
}

impl MigrationReportKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SparkMigration => "spark_migration",
            Self::DataFusionMigration => "datafusion_migration",
            Self::DuckDbPolarsMigration => "duckdb_polars_migration",
            Self::SqlCompatibility => "sql_compatibility",
            Self::PlanPortability => "plan_portability",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::SparkMigration,
            Self::DataFusionMigration,
            Self::DuckDbPolarsMigration,
            Self::SqlCompatibility,
            Self::PlanPortability,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationCompatibilityEntry {
    pub report_kind: MigrationReportKind,
    pub status: CapabilityCertificationStatus,
    pub supported_constructs: Vec<String>,
    pub unsupported_constructs: Vec<String>,
    pub semantic_differences: Vec<String>,
    pub rewrite_suggestions: Vec<String>,
    pub performance_cost_delta_estimate: Option<String>,
    pub vortex_conversion_payback_estimate: Option<String>,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl MigrationCompatibilityEntry {
    #[must_use]
    pub const fn planned(report_kind: MigrationReportKind) -> Self {
        Self {
            report_kind,
            status: CapabilityCertificationStatus::Planned,
            supported_constructs: Vec::new(),
            unsupported_constructs: Vec::new(),
            semantic_differences: Vec::new(),
            rewrite_suggestions: Vec::new(),
            performance_cost_delta_estimate: None,
            vortex_conversion_payback_estimate: None,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScorecardDimension {
    Correctness,
    Performance,
    Cost,
    MemorySafety,
    SqlCoverage,
    FunctionCoverage,
    OperatorCoverage,
    AdapterCoverage,
    ApiUsability,
    Observability,
    MigrationEase,
    DeploymentEase,
    NoFallbackIntegrity,
}

impl ScorecardDimension {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Correctness => "correctness",
            Self::Performance => "performance",
            Self::Cost => "cost",
            Self::MemorySafety => "memory_safety",
            Self::SqlCoverage => "sql_coverage",
            Self::FunctionCoverage => "function_coverage",
            Self::OperatorCoverage => "operator_coverage",
            Self::AdapterCoverage => "adapter_coverage",
            Self::ApiUsability => "api_usability",
            Self::Observability => "observability",
            Self::MigrationEase => "migration_ease",
            Self::DeploymentEase => "deployment_ease",
            Self::NoFallbackIntegrity => "no_fallback_integrity",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Correctness,
            Self::Performance,
            Self::Cost,
            Self::MemorySafety,
            Self::SqlCoverage,
            Self::FunctionCoverage,
            Self::OperatorCoverage,
            Self::AdapterCoverage,
            Self::ApiUsability,
            Self::Observability,
            Self::MigrationEase,
            Self::DeploymentEase,
            Self::NoFallbackIntegrity,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestChoiceScorecardEntry {
    pub dimension: ScorecardDimension,
    pub status: CapabilityCertificationStatus,
    pub evidence_label: String,
    pub fallback_attempted: bool,
}

impl BestChoiceScorecardEntry {
    #[must_use]
    pub fn not_certified(dimension: ScorecardDimension) -> Self {
        Self {
            dimension,
            status: CapabilityCertificationStatus::Planned,
            evidence_label: "not_certified".to_string(),
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestChoiceScorecard {
    pub schema_version: &'static str,
    pub workload_constitution: Vec<String>,
    pub dimensions: Vec<BestChoiceScorecardEntry>,
    pub claim_level: CapabilityCertificationStatus,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl BestChoiceScorecard {
    #[must_use]
    pub fn not_certified() -> Self {
        Self {
            schema_version: "shardloom.best_choice_scorecard.v1",
            workload_constitution: Vec::new(),
            dimensions: ScorecardDimension::all()
                .iter()
                .copied()
                .map(BestChoiceScorecardEntry::not_certified)
                .collect(),
            claim_level: CapabilityCertificationStatus::Planned,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub const fn can_publish_best_choice_claim(&self) -> bool {
        self.claim_level.can_satisfy_production_claim() && !self.fallback_attempted
    }
}

/// Workload-scoped CG-20 publication decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldClassSufficiencyDecision {
    NotCertified,
    PartialForWorkload,
    SufficientForWorkload,
    BestDefaultCandidate,
    BestDefaultCertified,
}

impl WorldClassSufficiencyDecision {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotCertified => "not_certified",
            Self::PartialForWorkload => "partial_for_workload",
            Self::SufficientForWorkload => "sufficient_for_workload",
            Self::BestDefaultCandidate => "best_default_candidate",
            Self::BestDefaultCertified => "best_default_certified",
        }
    }

    #[must_use]
    pub const fn allows_public_best_default_claim(&self) -> bool {
        matches!(self, Self::BestDefaultCertified)
    }
}

/// Per-dimension CG-20 evidence state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldClassSufficiencyStatus {
    NotCertified,
    Planned,
    EvidenceInsufficient,
    PartialForWorkload,
    Certified,
    Blocked,
    OutOfScope,
}

impl WorldClassSufficiencyStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotCertified => "not_certified",
            Self::Planned => "planned",
            Self::EvidenceInsufficient => "evidence_insufficient",
            Self::PartialForWorkload => "partial_for_workload",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
            Self::OutOfScope => "out_of_scope",
        }
    }

    #[must_use]
    pub const fn satisfies_required_dimension(&self) -> bool {
        matches!(self, Self::Certified)
    }
}

/// Required world-class capability dimensions that CG-20 must evaluate together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldClassSufficiencyDimensionKind {
    WorkloadConstitution,
    SqlSurface,
    OperatorSurface,
    FunctionSurface,
    AdapterSurface,
    SemanticProfiles,
    MigrationSurface,
    DataEtlSurface,
    PythonSurface,
    DataFrameQueryBuilder,
    NotebookExperience,
    UdfPlugin,
    UnstructuredMedia,
    UniversalAdapterCatalog,
    EventApiSaasAdapters,
    ApiSurface,
    ObservabilitySurface,
    DeploymentSurface,
    ExtensionSurface,
    SecurityGovernance,
    NativeIoCertificateCoverage,
    ExecutionCertificateCoverage,
    CorrectnessEvidence,
    SemanticConformance,
    BenchmarkEvidence,
    MemorySpill,
    CapabilitySnapshots,
    BestChoiceScorecard,
    BestDefaultDossier,
    NoFallbackIntegrity,
}

impl WorldClassSufficiencyDimensionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::WorkloadConstitution => "workload_constitution",
            Self::SqlSurface => "sql_surface",
            Self::OperatorSurface => "operator_surface",
            Self::FunctionSurface => "function_surface",
            Self::AdapterSurface => "adapter_surface",
            Self::SemanticProfiles => "semantic_profiles",
            Self::MigrationSurface => "migration_surface",
            Self::DataEtlSurface => "data_etl_surface",
            Self::PythonSurface => "python_surface",
            Self::DataFrameQueryBuilder => "dataframe_query_builder",
            Self::NotebookExperience => "notebook_experience",
            Self::UdfPlugin => "udf_plugin",
            Self::UnstructuredMedia => "unstructured_media",
            Self::UniversalAdapterCatalog => "universal_adapter_catalog",
            Self::EventApiSaasAdapters => "event_api_saas_adapters",
            Self::ApiSurface => "api_surface",
            Self::ObservabilitySurface => "observability_surface",
            Self::DeploymentSurface => "deployment_surface",
            Self::ExtensionSurface => "extension_surface",
            Self::SecurityGovernance => "security_governance",
            Self::NativeIoCertificateCoverage => "native_io_certificate_coverage",
            Self::ExecutionCertificateCoverage => "execution_certificate_coverage",
            Self::CorrectnessEvidence => "correctness_evidence",
            Self::SemanticConformance => "semantic_conformance",
            Self::BenchmarkEvidence => "benchmark_evidence",
            Self::MemorySpill => "memory_spill",
            Self::CapabilitySnapshots => "capability_snapshots",
            Self::BestChoiceScorecard => "best_choice_scorecard",
            Self::BestDefaultDossier => "best_default_dossier",
            Self::NoFallbackIntegrity => "no_fallback_integrity",
        }
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub const fn all() -> &'static [Self] {
        &[
            Self::WorkloadConstitution,
            Self::SqlSurface,
            Self::OperatorSurface,
            Self::FunctionSurface,
            Self::AdapterSurface,
            Self::SemanticProfiles,
            Self::MigrationSurface,
            Self::DataEtlSurface,
            Self::PythonSurface,
            Self::DataFrameQueryBuilder,
            Self::NotebookExperience,
            Self::UdfPlugin,
            Self::UnstructuredMedia,
            Self::UniversalAdapterCatalog,
            Self::EventApiSaasAdapters,
            Self::ApiSurface,
            Self::ObservabilitySurface,
            Self::DeploymentSurface,
            Self::ExtensionSurface,
            Self::SecurityGovernance,
            Self::NativeIoCertificateCoverage,
            Self::ExecutionCertificateCoverage,
            Self::CorrectnessEvidence,
            Self::SemanticConformance,
            Self::BenchmarkEvidence,
            Self::MemorySpill,
            Self::CapabilitySnapshots,
            Self::BestChoiceScorecard,
            Self::BestDefaultDossier,
            Self::NoFallbackIntegrity,
        ]
    }
}

/// One required CG-20 dimension and the evidence gates it must satisfy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct WorldClassSufficiencyDimension {
    pub kind: WorldClassSufficiencyDimensionKind,
    pub status: WorldClassSufficiencyStatus,
    pub required: bool,
    pub correctness_evidence_required: bool,
    pub semantic_conformance_required: bool,
    pub benchmark_evidence_required: bool,
    pub adapter_certification_required: bool,
    pub native_io_certificate_required: bool,
    pub execution_certificate_required: bool,
    pub capability_snapshot_required: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl WorldClassSufficiencyDimension {
    #[must_use]
    pub const fn required_planned(kind: WorldClassSufficiencyDimensionKind) -> Self {
        Self {
            kind,
            status: WorldClassSufficiencyStatus::EvidenceInsufficient,
            required: true,
            correctness_evidence_required: true,
            semantic_conformance_required: true,
            benchmark_evidence_required: true,
            adapter_certification_required: false,
            native_io_certificate_required: false,
            execution_certificate_required: false,
            capability_snapshot_required: true,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_adapter_certification_required(mut self) -> Self {
        self.adapter_certification_required = true;
        self
    }

    #[must_use]
    pub fn with_native_io_certificate_required(mut self) -> Self {
        self.native_io_certificate_required = true;
        self
    }

    #[must_use]
    pub fn with_execution_certificate_required(mut self) -> Self {
        self.execution_certificate_required = true;
        self
    }

    #[must_use]
    pub fn satisfies_required_dimension(&self) -> bool {
        !self.required
            || (self.status.satisfies_required_dimension()
                && !self.fallback_attempted
                && self.diagnostics.is_empty())
    }
}

/// Machine-readable CG-20 gate proving whether `ShardLoom` is world-class for a workload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct WorldClassSufficiencyReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub workload_constitution_ref: String,
    pub claim_level: WorldClassSufficiencyDecision,
    pub publication_decision: WorldClassSufficiencyDecision,
    pub dimensions: Vec<WorldClassSufficiencyDimension>,
    pub unsupported_rate: Option<String>,
    pub materialization_rate: Option<String>,
    pub performance_regression_budget_status: WorldClassSufficiencyStatus,
    pub scorecard_ref: String,
    pub best_default_dossier_ref: String,
    pub capability_snapshot_refs: Vec<String>,
    pub external_baseline_refs: Vec<String>,
    pub known_limits: Vec<String>,
    pub blocking_gaps: Vec<String>,
    pub runtime_execution: bool,
    pub parser_executed: bool,
    pub adapter_probe: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
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

impl WorldClassSufficiencyReport {
    /// Creates the report-only CG-20 foundation without evaluating real workloads.
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.world_class_sufficiency.v1",
            report_id: "cg20.world_class_sufficiency".to_string(),
            workload_constitution_ref: "workload_constitution.pending".to_string(),
            claim_level: WorldClassSufficiencyDecision::NotCertified,
            publication_decision: WorldClassSufficiencyDecision::NotCertified,
            dimensions: planned_world_class_sufficiency_dimensions(),
            unsupported_rate: None,
            materialization_rate: None,
            performance_regression_budget_status: WorldClassSufficiencyStatus::EvidenceInsufficient,
            scorecard_ref: "best_choice_scorecard.pending".to_string(),
            best_default_dossier_ref: "best_default_dossier.pending".to_string(),
            capability_snapshot_refs: vec![
                "capability_certification_report.pending".to_string(),
                "feature_footprint_report.pending".to_string(),
            ],
            external_baseline_refs: vec![
                "spark_baseline.reference_only".to_string(),
                "datafusion_baseline.reference_only".to_string(),
            ],
            known_limits: vec![
                "CG-20 is not implemented; this report declares required evidence only."
                    .to_string(),
                "SQL parsing, adapter runtime, Conda package publication, UDF runtime, media extraction, and execution remain deferred."
                    .to_string(),
            ],
            blocking_gaps: vec![
                "required world-class dimensions are evidence_insufficient".to_string(),
                "best-default publication is blocked until CG-5, CG-6, CG-16, and CG-19 evidence exists"
                    .to_string(),
            ],
            runtime_execution: false,
            parser_executed: false,
            adapter_probe: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
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
    pub fn dimension_count(&self) -> usize {
        self.dimensions.len()
    }

    #[must_use]
    pub fn required_dimension_count(&self) -> usize {
        self.dimensions
            .iter()
            .filter(|dimension| dimension.required)
            .count()
    }

    #[must_use]
    pub fn evidence_insufficient_dimension_count(&self) -> usize {
        self.dimensions
            .iter()
            .filter(|dimension| {
                dimension.status == WorldClassSufficiencyStatus::EvidenceInsufficient
            })
            .count()
    }

    #[must_use]
    pub fn dimension_kind_order(&self) -> String {
        self.dimensions
            .iter()
            .map(|dimension| dimension.kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn status_for(
        &self,
        kind: WorldClassSufficiencyDimensionKind,
    ) -> WorldClassSufficiencyStatus {
        self.dimensions
            .iter()
            .find(|dimension| dimension.kind == kind)
            .map_or(WorldClassSufficiencyStatus::NotCertified, |dimension| {
                dimension.status
            })
    }

    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution
            && !self.parser_executed
            && !self.adapter_probe
            && !self.filesystem_probe
            && !self.network_probe
            && !self.catalog_probe
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
    pub fn can_publish_best_default_claim(&self) -> bool {
        self.publication_decision.allows_public_best_default_claim()
            && self.production_claim_allowed
            && self.is_side_effect_free()
            && self
                .dimensions
                .iter()
                .all(WorldClassSufficiencyDimension::satisfies_required_dimension)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || !self.is_side_effect_free()
            || self
                .dimensions
                .iter()
                .any(|dimension| dimension.fallback_attempted)
            || (self.production_claim_allowed && !self.can_publish_best_default_claim())
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "claim_level: {}", self.claim_level.as_str());
        let _ = writeln!(
            out,
            "publication_decision: {}",
            self.publication_decision.as_str()
        );
        let _ = writeln!(out, "fallback execution: disabled");
        let _ = writeln!(out, "side effects: none");
        let _ = writeln!(out, "dimensions: {}", self.dimension_count());
        let _ = writeln!(
            out,
            "evidence_insufficient_dimensions: {}",
            self.evidence_insufficient_dimension_count()
        );
        let _ = writeln!(
            out,
            "best default claim: {}",
            if self.can_publish_best_default_claim() {
                "allowed"
            } else {
                "not_allowed"
            }
        );
        out
    }
}

#[must_use]
pub fn plan_world_class_sufficiency() -> WorldClassSufficiencyReport {
    WorldClassSufficiencyReport::contract_only()
}

/// Report-only GAR-0032-E gate for best-default language and publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BestDefaultCertificationGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub docs_ref: &'static str,
    pub source_refs: &'static str,
    pub support_status: &'static str,
    pub gate_status: &'static str,
    pub claim_gate_status: &'static str,
    pub required_evidence: &'static str,
    pub missing_evidence: &'static str,
    pub attached_evidence_refs: &'static str,
    pub blocker_ids: &'static str,
    pub correctness_evidence_required: bool,
    pub benchmark_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub materialization_decode_required: bool,
    pub no_fallback_policy_required: bool,
    pub release_security_required: bool,
    pub ux_install_docs_required: bool,
    pub all_required_evidence_attached: bool,
    pub best_default_language_allowed: bool,
    pub best_default_claim_allowed: bool,
    pub performance_claim_allowed: bool,
    pub superiority_claim_allowed: bool,
    pub spark_replacement_claim_allowed: bool,
    pub production_claim_allowed: bool,
    pub runtime_execution: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl BestDefaultCertificationGateReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: "shardloom.best_default_certification_gate.v1",
            report_id: "gar-0032-e.best_default_certification_gate",
            docs_ref: "docs/architecture/best-default-certification-gate.md",
            source_refs: "docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md,docs/architecture/operational-evidence-policy-hardening.md,docs/architecture/benchmark-suite-catalog.md",
            support_status: "blocked",
            gate_status: "blocked_missing_evidence",
            claim_gate_status: "not_claim_grade",
            required_evidence: "workload_constitution,correctness_evidence,benchmark_evidence,execution_certificate,native_io_certificate,materialization_decode,no_fallback_policy,release_security,ux_install_docs,capability_snapshot,best_choice_scorecard,best_default_dossier",
            missing_evidence: "workload_constitution,correctness_evidence,benchmark_evidence,execution_certificate,native_io_certificate,materialization_decode,no_fallback_policy,release_security,ux_install_docs,capability_snapshot,best_choice_scorecard,best_default_dossier",
            attached_evidence_refs: "none",
            blocker_ids: "gar-0032-e.missing_workload_constitution,gar-0032-e.missing_correctness_evidence,gar-0032-e.missing_benchmark_evidence,gar-0032-e.missing_certificates,gar-0032-e.missing_native_io,gar-0032-e.missing_materialization_decode,gar-0032-e.missing_release_security,gar-0032-e.missing_ux_install_docs",
            correctness_evidence_required: true,
            benchmark_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            materialization_decode_required: true,
            no_fallback_policy_required: true,
            release_security_required: true,
            ux_install_docs_required: true,
            all_required_evidence_attached: false,
            best_default_language_allowed: false,
            best_default_claim_allowed: false,
            performance_claim_allowed: false,
            superiority_claim_allowed: false,
            spark_replacement_claim_allowed: false,
            production_claim_allowed: false,
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary: "report_only_gate_no_best_default_performance_superiority_replacement_or_production_claim",
        }
    }
}

#[must_use]
pub const fn plan_best_default_certification_gate() -> BestDefaultCertificationGateReport {
    BestDefaultCertificationGateReport::report_only()
}

/// Broad CG-20 user-facing surfaces that must not be promoted by implication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCapabilityPromotionSurface {
    WorldClassSufficiencyFoundation,
    PythonWrapperFoundation,
    InputAdapterRegistryFoundation,
    UnstructuredWorkflowBoundaryContracts,
    SqlFrontendRuntime,
    DataFrameQueryBuilderRuntime,
    NotebookRuntime,
    UdfPluginRuntime,
    UnstructuredMediaEffectRuntime,
    UniversalAdapterRuntime,
    EventApiSaasAdapterRuntime,
    AdapterReadWriteCommitRuntime,
    SemanticProfileConformanceRuntime,
    WorkloadCertifiedCapabilityCloseout,
    BestDefaultDossierPublication,
}

impl UserCapabilityPromotionSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::WorldClassSufficiencyFoundation => "world_class_sufficiency_foundation",
            Self::PythonWrapperFoundation => "python_wrapper_foundation",
            Self::InputAdapterRegistryFoundation => "input_adapter_registry_foundation",
            Self::UnstructuredWorkflowBoundaryContracts => {
                "unstructured_workflow_boundary_contracts"
            }
            Self::SqlFrontendRuntime => "sql_frontend_runtime",
            Self::DataFrameQueryBuilderRuntime => "dataframe_query_builder_runtime",
            Self::NotebookRuntime => "notebook_runtime",
            Self::UdfPluginRuntime => "udf_plugin_runtime",
            Self::UnstructuredMediaEffectRuntime => "unstructured_media_effect_runtime",
            Self::UniversalAdapterRuntime => "universal_adapter_runtime",
            Self::EventApiSaasAdapterRuntime => "event_api_saas_adapter_runtime",
            Self::AdapterReadWriteCommitRuntime => "adapter_read_write_commit_runtime",
            Self::SemanticProfileConformanceRuntime => "semantic_profile_conformance_runtime",
            Self::WorkloadCertifiedCapabilityCloseout => "workload_certified_capability_closeout",
            Self::BestDefaultDossierPublication => "best_default_dossier_publication",
        }
    }
}

/// Status for the broad CG-20 promotion gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCapabilityPromotionStatus {
    ExistingReportOnlyEvidence,
    BlockedUntilCertified,
}

impl UserCapabilityPromotionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingReportOnlyEvidence => "existing_report_only_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }

    #[must_use]
    pub const fn is_existing_evidence(&self) -> bool {
        matches!(self, Self::ExistingReportOnlyEvidence)
    }
}

/// One CG-20 broad user-capability surface and the evidence needed before promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UserCapabilityPromotionGateEntry {
    pub surface: UserCapabilityPromotionSurface,
    pub status: UserCapabilityPromotionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub requires_world_class_sufficiency_report: bool,
    pub requires_semantic_profile: bool,
    pub requires_sql_coverage: bool,
    pub requires_operator_coverage: bool,
    pub requires_function_coverage: bool,
    pub requires_adapter_certification: bool,
    pub requires_native_io_certificate: bool,
    pub requires_execution_certificate: bool,
    pub requires_correctness_evidence: bool,
    pub requires_benchmark_evidence: bool,
    pub requires_workload_constitution: bool,
    pub requires_materialization_boundary: bool,
    pub requires_effect_policy: bool,
    pub requires_security_governance: bool,
    pub requires_protocol_surface_parity: bool,
    pub runtime_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
}

impl UserCapabilityPromotionGateEntry {
    #[must_use]
    pub const fn existing(
        surface: UserCapabilityPromotionSurface,
        existing_report_ref: &'static str,
    ) -> Self {
        Self {
            surface,
            status: UserCapabilityPromotionStatus::ExistingReportOnlyEvidence,
            existing_report_ref: Some(existing_report_ref),
            requires_world_class_sufficiency_report: false,
            requires_semantic_profile: false,
            requires_sql_coverage: false,
            requires_operator_coverage: false,
            requires_function_coverage: false,
            requires_adapter_certification: false,
            requires_native_io_certificate: false,
            requires_execution_certificate: false,
            requires_correctness_evidence: false,
            requires_benchmark_evidence: false,
            requires_workload_constitution: false,
            requires_materialization_boundary: false,
            requires_effect_policy: false,
            requires_security_governance: false,
            requires_protocol_surface_parity: false,
            runtime_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocked(surface: UserCapabilityPromotionSurface) -> Self {
        Self {
            surface,
            status: UserCapabilityPromotionStatus::BlockedUntilCertified,
            existing_report_ref: None,
            requires_world_class_sufficiency_report: true,
            requires_semantic_profile: true,
            requires_sql_coverage: true,
            requires_operator_coverage: true,
            requires_function_coverage: true,
            requires_adapter_certification: true,
            requires_native_io_certificate: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_benchmark_evidence: true,
            requires_workload_constitution: true,
            requires_materialization_boundary: true,
            requires_effect_policy: true,
            requires_security_governance: true,
            requires_protocol_surface_parity: true,
            runtime_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_allowed && !self.external_engine_invoked && !self.fallback_execution_allowed
    }
}

/// Report-only CG-20 gate for promoting broad user capability surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UserCapabilityPromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<UserCapabilityPromotionGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_world_class_sufficiency_report_present: bool,
    pub existing_python_wrapper_foundation_present: bool,
    pub existing_input_adapter_registry_present: bool,
    pub existing_unstructured_workflow_boundary_contracts_present: bool,
    pub sql_runtime_allowed: bool,
    pub dataframe_runtime_allowed: bool,
    pub notebook_runtime_allowed: bool,
    pub udf_execution_allowed: bool,
    pub plugin_execution_allowed: bool,
    pub unstructured_media_decode_allowed: bool,
    pub ocr_transcription_embedding_llm_allowed: bool,
    pub adapter_runtime_allowed: bool,
    pub external_api_call_allowed: bool,
    pub catalog_probe_allowed: bool,
    pub object_store_io_allowed: bool,
    pub write_io_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub best_default_claim_allowed: bool,
    pub user_capability_claim_allowed: bool,
    pub world_class_sufficiency_report_required: bool,
    pub semantic_profile_required: bool,
    pub sql_coverage_required: bool,
    pub operator_coverage_required: bool,
    pub function_coverage_required: bool,
    pub adapter_certification_required: bool,
    pub native_io_certificate_required: bool,
    pub execution_certificate_required: bool,
    pub correctness_evidence_required: bool,
    pub benchmark_evidence_required: bool,
    pub workload_constitution_required: bool,
    pub materialization_boundary_required: bool,
    pub effect_policy_required: bool,
    pub security_governance_required: bool,
    pub protocol_surface_parity_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl UserCapabilityPromotionGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.user_capability_promotion_gate.v1",
            report_id: "cg20.user_capability_promotion_gate",
            entries: user_capability_promotion_entries(),
            existing_report_refs: user_capability_existing_report_refs(),
            existing_world_class_sufficiency_report_present: true,
            existing_python_wrapper_foundation_present: true,
            existing_input_adapter_registry_present: true,
            existing_unstructured_workflow_boundary_contracts_present: true,
            sql_runtime_allowed: false,
            dataframe_runtime_allowed: false,
            notebook_runtime_allowed: false,
            udf_execution_allowed: false,
            plugin_execution_allowed: false,
            unstructured_media_decode_allowed: false,
            ocr_transcription_embedding_llm_allowed: false,
            adapter_runtime_allowed: false,
            external_api_call_allowed: false,
            catalog_probe_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            best_default_claim_allowed: false,
            user_capability_claim_allowed: false,
            world_class_sufficiency_report_required: true,
            semantic_profile_required: true,
            sql_coverage_required: true,
            operator_coverage_required: true,
            function_coverage_required: true,
            adapter_certification_required: true,
            native_io_certificate_required: true,
            execution_certificate_required: true,
            correctness_evidence_required: true,
            benchmark_evidence_required: true,
            workload_constitution_required: true,
            materialization_boundary_required: true,
            effect_policy_required: true,
            security_governance_required: true,
            protocol_surface_parity_required: true,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn existing_evidence_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_existing_evidence())
            .count()
    }

    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.status,
                    UserCapabilityPromotionStatus::BlockedUntilCertified
                )
            })
            .count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.sql_runtime_allowed
            && !self.dataframe_runtime_allowed
            && !self.notebook_runtime_allowed
            && !self.udf_execution_allowed
            && !self.plugin_execution_allowed
            && !self.unstructured_media_decode_allowed
            && !self.ocr_transcription_embedding_llm_allowed
            && !self.adapter_runtime_allowed
            && !self.external_api_call_allowed
            && !self.catalog_probe_allowed
            && !self.object_store_io_allowed
            && !self.write_io_allowed
            && !self.external_engine_invoked
            && self
                .entries
                .iter()
                .all(|entry| !entry.runtime_allowed && !entry.external_engine_invoked)
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.best_default_claim_allowed && !self.user_capability_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .entries
                .iter()
                .all(UserCapabilityPromotionGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "existing report refs: {}",
            self.existing_report_refs.join(",")
        );
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} runtime_allowed={} external_engine_invoked={} requires_world_class_sufficiency_report={} requires_semantic_profile={} requires_sql_coverage={} requires_function_coverage={} requires_adapter_certification={} requires_execution_certificate={} requires_native_io_certificate={} fallback_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.runtime_allowed,
                entry.external_engine_invoked,
                entry.requires_world_class_sufficiency_report,
                entry.requires_semantic_profile,
                entry.requires_sql_coverage,
                entry.requires_function_coverage,
                entry.requires_adapter_certification,
                entry.requires_execution_certificate,
                entry.requires_native_io_certificate,
                entry.fallback_execution_allowed
            );
        }
        out
    }
}

fn user_capability_promotion_entries() -> Vec<UserCapabilityPromotionGateEntry> {
    vec![
        UserCapabilityPromotionGateEntry::existing(
            UserCapabilityPromotionSurface::WorldClassSufficiencyFoundation,
            "world-class-sufficiency-plan",
        ),
        UserCapabilityPromotionGateEntry::existing(
            UserCapabilityPromotionSurface::PythonWrapperFoundation,
            "python-wrapper-plan",
        ),
        UserCapabilityPromotionGateEntry::existing(
            UserCapabilityPromotionSurface::InputAdapterRegistryFoundation,
            "input-adapters",
        ),
        UserCapabilityPromotionGateEntry::existing(
            UserCapabilityPromotionSurface::UnstructuredWorkflowBoundaryContracts,
            "cg21p.unstructured_workflow_boundaries",
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::SqlFrontendRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::DataFrameQueryBuilderRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(UserCapabilityPromotionSurface::NotebookRuntime),
        UserCapabilityPromotionGateEntry::blocked(UserCapabilityPromotionSurface::UdfPluginRuntime),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::UnstructuredMediaEffectRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::UniversalAdapterRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::EventApiSaasAdapterRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::AdapterReadWriteCommitRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::SemanticProfileConformanceRuntime,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::WorkloadCertifiedCapabilityCloseout,
        ),
        UserCapabilityPromotionGateEntry::blocked(
            UserCapabilityPromotionSurface::BestDefaultDossierPublication,
        ),
    ]
}

fn user_capability_existing_report_refs() -> Vec<&'static str> {
    vec![
        "world-class-sufficiency-plan",
        "capabilities certification",
        "python-wrapper-plan",
        "input-adapters",
        "native-io-envelope-plan",
        "execution-certificate-plan",
        "cg21p.unstructured_workflow_boundaries",
        "operational_contracts.protocol_surface_parity",
    ]
}

#[must_use]
pub fn plan_user_capability_promotion_gate() -> UserCapabilityPromotionGateReport {
    UserCapabilityPromotionGateReport::planning_default()
}

fn planned_world_class_sufficiency_dimensions() -> Vec<WorldClassSufficiencyDimension> {
    WorldClassSufficiencyDimensionKind::all()
        .iter()
        .copied()
        .map(planned_world_class_sufficiency_dimension)
        .collect()
}

fn planned_world_class_sufficiency_dimension(
    kind: WorldClassSufficiencyDimensionKind,
) -> WorldClassSufficiencyDimension {
    let dimension = WorldClassSufficiencyDimension::required_planned(kind);
    match kind {
        WorldClassSufficiencyDimensionKind::AdapterSurface
        | WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog
        | WorldClassSufficiencyDimensionKind::EventApiSaasAdapters => {
            dimension.with_adapter_certification_required()
        }
        WorldClassSufficiencyDimensionKind::NativeIoCertificateCoverage
        | WorldClassSufficiencyDimensionKind::DataEtlSurface
        | WorldClassSufficiencyDimensionKind::UnstructuredMedia => {
            dimension.with_native_io_certificate_required()
        }
        WorldClassSufficiencyDimensionKind::ExecutionCertificateCoverage
        | WorldClassSufficiencyDimensionKind::OperatorSurface
        | WorldClassSufficiencyDimensionKind::MemorySpill => {
            dimension.with_execution_certificate_required()
        }
        _ => dimension,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCertificationReport {
    pub schema_version: &'static str,
    pub engine_version: String,
    pub sql_coverage: SqlCoverageMatrix,
    pub operator_coverage: OperatorCoverageMatrix,
    pub function_coverage: FunctionCoverageMatrix,
    pub adapter_certification: AdapterCertificationMatrix,
    pub semantic_profiles: Vec<SemanticProfileEntry>,
    pub migration_reports: Vec<MigrationCompatibilityEntry>,
    pub best_choice_scorecard: BestChoiceScorecard,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl CapabilityCertificationReport {
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.capability_certification.v1",
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            sql_coverage: SqlCoverageMatrix::planned_foundation(),
            operator_coverage: OperatorCoverageMatrix::planned_foundation(),
            function_coverage: FunctionCoverageMatrix::planned_foundation(),
            adapter_certification: AdapterCertificationMatrix::planned_foundation(),
            semantic_profiles: SemanticProfileName::all()
                .iter()
                .copied()
                .map(SemanticProfileEntry::planned)
                .collect(),
            migration_reports: MigrationReportKind::all()
                .iter()
                .copied()
                .map(MigrationCompatibilityEntry::planned)
                .collect(),
            best_choice_scorecard: BestChoiceScorecard::not_certified(),
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }

    #[must_use]
    pub fn can_publish_best_choice_claim(&self) -> bool {
        self.best_choice_scorecard.can_publish_best_choice_claim()
            && !self.fallback_attempted
            && self
                .sql_coverage
                .entries
                .iter()
                .all(|entry| entry.can_satisfy_production_claim() && !entry.fallback_attempted)
            && self.operator_coverage.entries.iter().all(|entry| {
                entry.status.can_satisfy_production_claim() && !entry.fallback_attempted
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "engine_version: {}", self.engine_version);
        let _ = writeln!(out, "fallback execution: disabled");
        let _ = writeln!(out, "sql features: {}", self.sql_coverage.entries.len());
        let _ = writeln!(
            out,
            "operator families: {}",
            self.operator_coverage.entries.len()
        );
        let _ = writeln!(
            out,
            "function groups: {}",
            self.function_coverage.entries.len()
        );
        let _ = writeln!(
            out,
            "adapter entries: {}",
            self.adapter_certification.entries.len()
        );
        let _ = writeln!(
            out,
            "best choice claim: {}",
            if self.can_publish_best_choice_claim() {
                "certified"
            } else {
                "not_certified"
            }
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_rejects_empty_name() {
        assert!(
            CapabilityCertificationEntry::new(
                " ",
                CapabilityCertificationSurface::Sql,
                CapabilityCertificationStatus::Planned,
            )
            .is_err()
        );
    }

    #[test]
    fn test_reference_only_cannot_satisfy_production_claim() {
        assert!(CapabilityCertificationStatus::TestReferenceOnly.is_reference_only());
        assert!(!CapabilityCertificationStatus::TestReferenceOnly.has_native_capability());
        assert!(!CapabilityCertificationStatus::TestReferenceOnly.can_satisfy_production_claim());
        assert!(OperatorCertificationStatus::TestReferenceOnly.is_reference_only());
        assert!(!OperatorCertificationStatus::TestReferenceOnly.can_satisfy_production_claim());
    }

    #[test]
    fn planned_sql_entries_do_not_satisfy_claims() {
        let matrix = SqlCoverageMatrix::planned_foundation();
        assert!(
            matrix
                .entries
                .iter()
                .any(|entry| entry.feature == SqlFeatureGroup::Select)
        );
        assert!(
            matrix
                .entries
                .iter()
                .all(|entry| !entry.can_satisfy_production_claim())
        );
        assert!(!matrix.fallback_attempted);
    }

    #[test]
    fn sql_dataframe_planner_readiness_is_report_only_and_non_executing() {
        let matrix = SqlDataFramePlannerReadinessMatrix::report_only();

        assert_eq!(
            matrix.schema_version,
            "shardloom.sql_dataframe_planner_readiness.v1"
        );
        assert_eq!(
            matrix.sql_row_order(),
            vec![
                "sql_text_admission",
                "sql_parse",
                "sql_bind",
                "sql_plan",
                "sql_execute",
            ]
        );
        assert_eq!(
            matrix.dataframe_row_order(),
            vec![
                "dataframe_lazy_plan",
                "dataframe_expression_builder",
                "dataframe_join",
                "dataframe_aggregate",
                "dataframe_window",
            ]
        );
        assert!(matrix.all_rows_fallback_free());
        assert!(matrix.all_rows_not_claim_grade());
        assert!(matrix.all_rows_non_executing());
        assert!(matrix.deterministic_diagnostics_present());
        assert!(
            matrix
                .unsupported_diagnostic_codes()
                .contains(&"SL_UNSUPPORTED_SQL")
        );
    }

    #[test]
    fn planned_operator_matrix_includes_join_window_shuffle_blockers() {
        let matrix = OperatorCoverageMatrix::planned_foundation();
        assert!(
            matrix
                .entries
                .iter()
                .any(|entry| entry.family == OperatorFamily::Join)
        );
        assert!(
            matrix
                .entries
                .iter()
                .any(|entry| entry.family == OperatorFamily::Window)
        );
        assert!(
            matrix
                .entries
                .iter()
                .any(|entry| entry.family == OperatorFamily::ShuffleExchange)
        );
        assert!(matrix.entries.iter().all(|entry| !entry.fallback_attempted));
    }

    #[test]
    fn adapter_declared_only_does_not_imply_read_support() {
        let adapter =
            AdapterCertificationEntry::planned("native_vortex", "native_vortex").expect("valid");
        assert_eq!(adapter.maturity, AdapterMaturityLevel::DeclaredOnly);
        assert!(!adapter.maturity.can_read());
        assert!(!adapter.read_supported);
        assert!(!adapter.fallback_attempted);
    }

    #[test]
    fn contract_only_report_is_not_certified_and_has_no_fallback() {
        let report = CapabilityCertificationReport::contract_only();
        assert!(!report.fallback_attempted());
        assert!(!report.can_publish_best_choice_claim());
        assert!(report.semantic_profiles.iter().any(|entry| {
            entry.profile == SemanticProfileName::ShardLoomNative
                && entry.status == CapabilityCertificationStatus::Planned
        }));
        assert!(
            report
                .function_coverage
                .entries
                .iter()
                .any(|entry| entry.group == FunctionCoverageGroup::String)
        );
        assert!(
            report
                .adapter_certification
                .entries
                .iter()
                .any(|entry| entry.adapter_id == "native_vortex")
        );
    }

    #[test]
    fn world_class_sufficiency_report_is_report_only_and_not_certified() {
        let report = plan_world_class_sufficiency();
        assert_eq!(
            report.schema_version,
            "shardloom.world_class_sufficiency.v1"
        );
        assert_eq!(
            report.claim_level,
            WorldClassSufficiencyDecision::NotCertified
        );
        assert_eq!(
            report.publication_decision,
            WorldClassSufficiencyDecision::NotCertified
        );
        assert_eq!(report.dimension_count(), 30);
        assert_eq!(report.required_dimension_count(), 30);
        assert_eq!(report.evidence_insufficient_dimension_count(), 30);
        assert!(report.is_side_effect_free());
        assert!(!report.can_publish_best_default_claim());
        assert!(!report.has_errors());
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn world_class_sufficiency_includes_broad_user_capability_dimensions() {
        let report = plan_world_class_sufficiency();
        for expected in [
            WorldClassSufficiencyDimensionKind::SqlSurface,
            WorldClassSufficiencyDimensionKind::OperatorSurface,
            WorldClassSufficiencyDimensionKind::FunctionSurface,
            WorldClassSufficiencyDimensionKind::AdapterSurface,
            WorldClassSufficiencyDimensionKind::DataEtlSurface,
            WorldClassSufficiencyDimensionKind::PythonSurface,
            WorldClassSufficiencyDimensionKind::DataFrameQueryBuilder,
            WorldClassSufficiencyDimensionKind::NotebookExperience,
            WorldClassSufficiencyDimensionKind::UdfPlugin,
            WorldClassSufficiencyDimensionKind::UnstructuredMedia,
            WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog,
            WorldClassSufficiencyDimensionKind::EventApiSaasAdapters,
            WorldClassSufficiencyDimensionKind::NativeIoCertificateCoverage,
            WorldClassSufficiencyDimensionKind::ExecutionCertificateCoverage,
            WorldClassSufficiencyDimensionKind::NoFallbackIntegrity,
        ] {
            assert_eq!(
                report.status_for(expected),
                WorldClassSufficiencyStatus::EvidenceInsufficient,
                "missing expected CG-20 dimension: {}",
                expected.as_str()
            );
        }
    }

    #[test]
    fn world_class_sufficiency_flags_side_effect_and_claim_violations() {
        let mut report = plan_world_class_sufficiency();
        report.runtime_execution = true;
        assert!(!report.is_side_effect_free());
        assert!(report.has_errors());

        let mut report = plan_world_class_sufficiency();
        report.production_claim_allowed = true;
        assert!(report.has_errors());
        assert!(!report.can_publish_best_default_claim());
    }

    #[test]
    fn user_capability_promotion_gate_keeps_broad_surfaces_blocked() {
        let report = plan_user_capability_promotion_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.user_capability_promotion_gate.v1"
        );
        assert_eq!(report.surface_count(), 15);
        assert_eq!(report.existing_evidence_surface_count(), 4);
        assert_eq!(report.blocked_surface_count(), 11);
        assert_eq!(
            report.surface_order(),
            vec![
                "world_class_sufficiency_foundation",
                "python_wrapper_foundation",
                "input_adapter_registry_foundation",
                "unstructured_workflow_boundary_contracts",
                "sql_frontend_runtime",
                "dataframe_query_builder_runtime",
                "notebook_runtime",
                "udf_plugin_runtime",
                "unstructured_media_effect_runtime",
                "universal_adapter_runtime",
                "event_api_saas_adapter_runtime",
                "adapter_read_write_commit_runtime",
                "semantic_profile_conformance_runtime",
                "workload_certified_capability_closeout",
                "best_default_dossier_publication",
            ]
        );
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn user_capability_promotion_gate_requires_evidence_before_user_claims() {
        let report = plan_user_capability_promotion_gate();
        assert!(report.existing_world_class_sufficiency_report_present);
        assert!(report.existing_python_wrapper_foundation_present);
        assert!(report.existing_input_adapter_registry_present);
        assert!(report.existing_unstructured_workflow_boundary_contracts_present);
        assert!(report.world_class_sufficiency_report_required);
        assert!(report.semantic_profile_required);
        assert!(report.sql_coverage_required);
        assert!(report.operator_coverage_required);
        assert!(report.function_coverage_required);
        assert!(report.adapter_certification_required);
        assert!(report.native_io_certificate_required);
        assert!(report.execution_certificate_required);
        assert!(report.correctness_evidence_required);
        assert!(report.benchmark_evidence_required);
        assert!(report.workload_constitution_required);
        assert!(report.materialization_boundary_required);
        assert!(report.effect_policy_required);
        assert!(report.security_governance_required);
        assert!(report.protocol_surface_parity_required);
        assert!(!report.sql_runtime_allowed);
        assert!(!report.dataframe_runtime_allowed);
        assert!(!report.udf_execution_allowed);
        assert!(!report.unstructured_media_decode_allowed);
        assert!(!report.ocr_transcription_embedding_llm_allowed);
        assert!(!report.adapter_runtime_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.user_capability_claim_allowed);
        assert!(!report.best_default_claim_allowed);
    }

    #[test]
    fn human_text_mentions_not_certified_and_fallback_disabled() {
        let text = CapabilityCertificationReport::contract_only().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("best choice claim: not_certified"));
    }
}
