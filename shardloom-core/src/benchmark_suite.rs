//! Local-first benchmark suite architecture and coverage reporting.
//!
//! This module records the CG-6.25 benchmark-suite shape without running
//! benchmarks. External engines remain comparison-only and never fallback.

use crate::operational_contracts::BenchmarkConstitution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkSuiteKind {
    Common,
    LocalAnalytics,
    NativeVortex,
    EtlWorkflows,
    SourceBackedEncoded,
    LayoutAndPruning,
    IncrementalState,
    Stress,
}

impl BenchmarkSuiteKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Common => "common",
            Self::LocalAnalytics => "local_analytics",
            Self::NativeVortex => "native_vortex",
            Self::EtlWorkflows => "etl_workflows",
            Self::SourceBackedEncoded => "source_backed_encoded",
            Self::LayoutAndPruning => "layout_and_pruning",
            Self::IncrementalState => "incremental_state",
            Self::Stress => "stress",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkScenarioCategory {
    ScanAndPruning,
    ProjectionAndLayout,
    Aggregation,
    Joins,
    SortAndWindow,
    EtlWrite,
    MessyLakehouseData,
    IncrementalState,
    OperationalCacheConcurrency,
}

impl BenchmarkScenarioCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScanAndPruning => "scan_and_pruning",
            Self::ProjectionAndLayout => "projection_and_layout",
            Self::Aggregation => "aggregation",
            Self::Joins => "joins",
            Self::SortAndWindow => "sort_and_window",
            Self::EtlWrite => "etl_write",
            Self::MessyLakehouseData => "messy_lakehouse_data",
            Self::IncrementalState => "incremental_state",
            Self::OperationalCacheConcurrency => "operational_cache_concurrency",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkSuiteDatasetProfileKind {
    TinySmoke,
    NarrowFactDim,
    WideTable,
    VeryWideTable,
    HighCardinalityStrings,
    NullHeavy,
    SkewedKeys,
    ManySmallFiles,
    FewLargeFiles,
    PartitionedByDate,
    PoorlyClustered,
    WellClustered,
    SchemaDrift,
    DirtyCsv,
    NestedJson,
    CdcDeltaOverlay,
}

impl BenchmarkSuiteDatasetProfileKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TinySmoke => "tiny_smoke",
            Self::NarrowFactDim => "narrow_fact_dim",
            Self::WideTable => "wide_table",
            Self::VeryWideTable => "very_wide_table",
            Self::HighCardinalityStrings => "high_cardinality_strings",
            Self::NullHeavy => "null_heavy",
            Self::SkewedKeys => "skewed_keys",
            Self::ManySmallFiles => "many_small_files",
            Self::FewLargeFiles => "few_large_files",
            Self::PartitionedByDate => "partitioned_by_date",
            Self::PoorlyClustered => "poorly_clustered",
            Self::WellClustered => "well_clustered",
            Self::SchemaDrift => "schema_drift",
            Self::DirtyCsv => "dirty_csv",
            Self::NestedJson => "nested_json",
            Self::CdcDeltaOverlay => "cdc_delta_overlay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkEngineRole {
    ShardLoomNative,
    LocalBaseline,
    VortexIntegrationBaseline,
    ExternalOracleReference,
    ManagedPlatformDesignReference,
}

impl BenchmarkEngineRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ShardLoomNative => "shardloom_native",
            Self::LocalBaseline => "local_baseline",
            Self::VortexIntegrationBaseline => "vortex_integration_baseline",
            Self::ExternalOracleReference => "external_oracle_reference",
            Self::ManagedPlatformDesignReference => "managed_platform_design_reference",
        }
    }

    #[must_use]
    pub const fn runtime_fallback_allowed(self) -> bool {
        false
    }

    #[must_use]
    pub const fn managed_platform_dependency_allowed(self) -> bool {
        !matches!(self, Self::ManagedPlatformDesignReference)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkCoverageStatus {
    Certified,
    Supported,
    Planned,
    Unsupported,
    Blocked,
    ExternalBaselineOnly,
}

impl BenchmarkCoverageStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::Supported => "supported",
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
            Self::Blocked => "blocked",
            Self::ExternalBaselineOnly => "external_baseline_only",
        }
    }

    #[must_use]
    pub const fn permits_shardloom_claim(self) -> bool {
        matches!(self, Self::Certified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkEnginePluginContract {
    pub engine_id: &'static str,
    pub role: BenchmarkEngineRole,
    pub dependency_required_by_core: bool,
    pub supported_formats_declared: bool,
    pub supported_scenarios_declared: bool,
    pub startup_policy_declared: bool,
    pub materialization_policy_declared: bool,
    pub result_policy_declared: bool,
    pub fallback_attempted: bool,
}

impl BenchmarkEnginePluginContract {
    #[must_use]
    pub const fn local(engine_id: &'static str, role: BenchmarkEngineRole) -> Self {
        Self {
            engine_id,
            role,
            dependency_required_by_core: false,
            supported_formats_declared: true,
            supported_scenarios_declared: true,
            startup_policy_declared: true,
            materialization_policy_declared: true,
            result_policy_declared: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn is_fallback_free(&self) -> bool {
        !self.dependency_required_by_core
            && !self.role.runtime_fallback_allowed()
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkCoverageTableRow {
    pub scenario_category: BenchmarkScenarioCategory,
    pub engine_id: &'static str,
    pub status: BenchmarkCoverageStatus,
    pub timing_required: bool,
    pub support_coverage_required: bool,
    pub certificate_status_required: bool,
    pub native_io_status_required: bool,
    pub materialization_decode_status_required: bool,
    pub fallback_attempted: bool,
}

impl BenchmarkCoverageTableRow {
    #[must_use]
    pub const fn blocked(
        scenario_category: BenchmarkScenarioCategory,
        engine_id: &'static str,
    ) -> Self {
        Self {
            scenario_category,
            engine_id,
            status: BenchmarkCoverageStatus::Blocked,
            timing_required: true,
            support_coverage_required: true,
            certificate_status_required: true,
            native_io_status_required: true,
            materialization_decode_status_required: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn external_baseline(
        scenario_category: BenchmarkScenarioCategory,
        engine_id: &'static str,
    ) -> Self {
        Self {
            scenario_category,
            engine_id,
            status: BenchmarkCoverageStatus::ExternalBaselineOnly,
            timing_required: true,
            support_coverage_required: true,
            certificate_status_required: false,
            native_io_status_required: false,
            materialization_decode_status_required: true,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkResultSchemaV2Report {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub benchmark_suite_required: bool,
    pub scenario_category_required: bool,
    pub dataset_profile_required: bool,
    pub engine_role_required: bool,
    pub compute_mode_required: bool,
    pub storage_mode_required: bool,
    pub timing_scope_required: bool,
    pub coverage_status_required: bool,
    pub execution_provider_kind_required: bool,
    pub residual_executor_required: bool,
    pub representation_transitions_required: bool,
    pub certificate_status_required: bool,
    pub native_io_status_required: bool,
    pub materialization_decode_status_required: bool,
    pub fallback_attempted_required: bool,
    pub external_engine_invoked_required: bool,
}

impl BenchmarkResultSchemaV2Report {
    #[must_use]
    pub const fn required() -> Self {
        Self {
            schema_version: "shardloom.benchmark_result_schema.v2",
            report_id: "cg6_25.benchmark_result_schema_v2.required",
            benchmark_suite_required: true,
            scenario_category_required: true,
            dataset_profile_required: true,
            engine_role_required: true,
            compute_mode_required: true,
            storage_mode_required: true,
            timing_scope_required: true,
            coverage_status_required: true,
            execution_provider_kind_required: true,
            residual_executor_required: true,
            representation_transitions_required: true,
            certificate_status_required: true,
            native_io_status_required: true,
            materialization_decode_status_required: true,
            fallback_attempted_required: true,
            external_engine_invoked_required: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkConstitutionRequirementReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub scenario_id_required: bool,
    pub scenario_category_required: bool,
    pub dataset_profile_required: bool,
    pub engine_role_required: bool,
    pub input_format_required: bool,
    pub table_format_required: bool,
    pub storage_mode_required: bool,
    pub native_vortex_or_compatibility_import_required: bool,
    pub startup_included_required: bool,
    pub conversion_included_required: bool,
    pub staging_included_required: bool,
    pub result_delivery_included_required: bool,
    pub write_included_required: bool,
    pub cache_mode_required: bool,
    pub iterations_required: bool,
    pub warmup_policy_required: bool,
    pub correctness_oracle_required: bool,
    pub materialization_policy_required: bool,
    pub resource_policy_required: bool,
    pub claim_level_required: bool,
}

impl BenchmarkConstitutionRequirementReport {
    #[must_use]
    pub const fn required() -> Self {
        Self {
            schema_version: "shardloom.benchmark_constitution_requirements.v1",
            report_id: "cg6_25.benchmark_constitution_requirements.required",
            scenario_id_required: true,
            scenario_category_required: true,
            dataset_profile_required: true,
            engine_role_required: true,
            input_format_required: true,
            table_format_required: true,
            storage_mode_required: true,
            native_vortex_or_compatibility_import_required: true,
            startup_included_required: true,
            conversion_included_required: true,
            staging_included_required: true,
            result_delivery_included_required: true,
            write_included_required: true,
            cache_mode_required: true,
            iterations_required: true,
            warmup_policy_required: true,
            correctness_oracle_required: true,
            materialization_policy_required: true,
            resource_policy_required: true,
            claim_level_required: true,
        }
    }

    #[must_use]
    pub const fn covers_rfc_0040_fields(&self) -> bool {
        self.scenario_id_required
            && self.scenario_category_required
            && self.dataset_profile_required
            && self.engine_role_required
            && self.input_format_required
            && self.table_format_required
            && self.storage_mode_required
            && self.native_vortex_or_compatibility_import_required
            && self.startup_included_required
            && self.conversion_included_required
            && self.staging_included_required
            && self.result_delivery_included_required
            && self.write_included_required
            && self.cache_mode_required
            && self.iterations_required
            && self.warmup_policy_required
            && self.correctness_oracle_required
            && self.materialization_policy_required
            && self.resource_policy_required
            && self.claim_level_required
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BenchmarkSuiteCatalogReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub suite_order: Vec<BenchmarkSuiteKind>,
    pub scenario_category_order: Vec<BenchmarkScenarioCategory>,
    pub dataset_profile_order: Vec<BenchmarkSuiteDatasetProfileKind>,
    pub engine_plugin_contracts: Vec<BenchmarkEnginePluginContract>,
    pub coverage_rows: Vec<BenchmarkCoverageTableRow>,
    pub benchmark_constitution_template: BenchmarkConstitution,
    pub benchmark_constitution_requirements: BenchmarkConstitutionRequirementReport,
    pub result_schema_v2: BenchmarkResultSchemaV2Report,
    pub platform_specific_managed_engines_excluded: bool,
    pub external_engines_benchmark_only: bool,
    pub plugin_based_optional_engines_required: bool,
    pub timing_and_coverage_separated: bool,
    pub benchmark_execution_performed: bool,
    pub managed_platform_dependency_added: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl BenchmarkSuiteCatalogReport {
    #[must_use]
    pub fn local_first_platform_neutral() -> Self {
        Self {
            schema_version: "shardloom.benchmark_suite_catalog.v1",
            report_id: "cg6_25.local_first_platform_neutral_benchmark_suite",
            suite_order: all_suite_kinds(),
            scenario_category_order: all_scenario_categories(),
            dataset_profile_order: all_dataset_profiles(),
            engine_plugin_contracts: local_engine_plugins(),
            coverage_rows: seed_coverage_rows(),
            benchmark_constitution_template: BenchmarkConstitution::report_only_foundation(),
            benchmark_constitution_requirements: BenchmarkConstitutionRequirementReport::required(),
            result_schema_v2: BenchmarkResultSchemaV2Report::required(),
            platform_specific_managed_engines_excluded: true,
            external_engines_benchmark_only: true,
            plugin_based_optional_engines_required: true,
            timing_and_coverage_separated: true,
            benchmark_execution_performed: false,
            managed_platform_dependency_added: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn all_engine_plugins_fallback_free(&self) -> bool {
        self.engine_plugin_contracts
            .iter()
            .all(BenchmarkEnginePluginContract::is_fallback_free)
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_required_taxonomy_coverage(&self) -> bool {
        self.suite_order.len() == all_suite_kinds().len()
            && self.scenario_category_order.len() == all_scenario_categories().len()
            && self.dataset_profile_order.len() == all_dataset_profiles().len()
    }

    #[must_use]
    pub fn managed_platforms_are_design_references_only(&self) -> bool {
        self.platform_specific_managed_engines_excluded
            && !self.managed_platform_dependency_added
            && self.engine_plugin_contracts.iter().all(|contract| {
                contract.role != BenchmarkEngineRole::ManagedPlatformDesignReference
            })
    }
}

#[must_use]
pub fn plan_benchmark_suite_catalog() -> BenchmarkSuiteCatalogReport {
    BenchmarkSuiteCatalogReport::local_first_platform_neutral()
}

fn all_suite_kinds() -> Vec<BenchmarkSuiteKind> {
    vec![
        BenchmarkSuiteKind::Common,
        BenchmarkSuiteKind::LocalAnalytics,
        BenchmarkSuiteKind::NativeVortex,
        BenchmarkSuiteKind::EtlWorkflows,
        BenchmarkSuiteKind::SourceBackedEncoded,
        BenchmarkSuiteKind::LayoutAndPruning,
        BenchmarkSuiteKind::IncrementalState,
        BenchmarkSuiteKind::Stress,
    ]
}

fn all_scenario_categories() -> Vec<BenchmarkScenarioCategory> {
    vec![
        BenchmarkScenarioCategory::ScanAndPruning,
        BenchmarkScenarioCategory::ProjectionAndLayout,
        BenchmarkScenarioCategory::Aggregation,
        BenchmarkScenarioCategory::Joins,
        BenchmarkScenarioCategory::SortAndWindow,
        BenchmarkScenarioCategory::EtlWrite,
        BenchmarkScenarioCategory::MessyLakehouseData,
        BenchmarkScenarioCategory::IncrementalState,
        BenchmarkScenarioCategory::OperationalCacheConcurrency,
    ]
}

fn all_dataset_profiles() -> Vec<BenchmarkSuiteDatasetProfileKind> {
    vec![
        BenchmarkSuiteDatasetProfileKind::TinySmoke,
        BenchmarkSuiteDatasetProfileKind::NarrowFactDim,
        BenchmarkSuiteDatasetProfileKind::WideTable,
        BenchmarkSuiteDatasetProfileKind::VeryWideTable,
        BenchmarkSuiteDatasetProfileKind::HighCardinalityStrings,
        BenchmarkSuiteDatasetProfileKind::NullHeavy,
        BenchmarkSuiteDatasetProfileKind::SkewedKeys,
        BenchmarkSuiteDatasetProfileKind::ManySmallFiles,
        BenchmarkSuiteDatasetProfileKind::FewLargeFiles,
        BenchmarkSuiteDatasetProfileKind::PartitionedByDate,
        BenchmarkSuiteDatasetProfileKind::PoorlyClustered,
        BenchmarkSuiteDatasetProfileKind::WellClustered,
        BenchmarkSuiteDatasetProfileKind::SchemaDrift,
        BenchmarkSuiteDatasetProfileKind::DirtyCsv,
        BenchmarkSuiteDatasetProfileKind::NestedJson,
        BenchmarkSuiteDatasetProfileKind::CdcDeltaOverlay,
    ]
}

fn local_engine_plugins() -> Vec<BenchmarkEnginePluginContract> {
    vec![
        BenchmarkEnginePluginContract::local("shardloom", BenchmarkEngineRole::ShardLoomNative),
        BenchmarkEnginePluginContract::local(
            "shardloom_native_vortex",
            BenchmarkEngineRole::ShardLoomNative,
        ),
        BenchmarkEnginePluginContract::local("pandas", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local("polars", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local("duckdb", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local("datafusion", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local("dask", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local("spark_default", BenchmarkEngineRole::LocalBaseline),
        BenchmarkEnginePluginContract::local(
            "spark_local_tuned",
            BenchmarkEngineRole::LocalBaseline,
        ),
        BenchmarkEnginePluginContract::local(
            "vortex_datafusion_integration",
            BenchmarkEngineRole::VortexIntegrationBaseline,
        ),
        BenchmarkEnginePluginContract::local(
            "vortex_duckdb_integration",
            BenchmarkEngineRole::VortexIntegrationBaseline,
        ),
    ]
}

fn seed_coverage_rows() -> Vec<BenchmarkCoverageTableRow> {
    vec![
        BenchmarkCoverageTableRow::blocked(BenchmarkScenarioCategory::ScanAndPruning, "shardloom"),
        BenchmarkCoverageTableRow::blocked(
            BenchmarkScenarioCategory::ProjectionAndLayout,
            "shardloom",
        ),
        BenchmarkCoverageTableRow::blocked(BenchmarkScenarioCategory::EtlWrite, "shardloom"),
        BenchmarkCoverageTableRow::external_baseline(
            BenchmarkScenarioCategory::ScanAndPruning,
            "duckdb",
        ),
        BenchmarkCoverageTableRow::external_baseline(
            BenchmarkScenarioCategory::ScanAndPruning,
            "polars",
        ),
        BenchmarkCoverageTableRow::external_baseline(
            BenchmarkScenarioCategory::ScanAndPruning,
            "vortex_datafusion_integration",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_suite_catalog_is_local_first_and_platform_neutral() {
        let report = plan_benchmark_suite_catalog();

        assert!(report.has_required_taxonomy_coverage());
        assert!(report.managed_platforms_are_design_references_only());
        assert!(report.external_engines_benchmark_only);
        assert!(!report.benchmark_execution_performed);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn engine_plugins_are_optional_and_never_fallback() {
        let report = plan_benchmark_suite_catalog();

        assert!(report.all_engine_plugins_fallback_free());
        assert!(report.engine_plugin_contracts.iter().any(|plugin| {
            plugin.engine_id == "vortex_datafusion_integration"
                && plugin.role == BenchmarkEngineRole::VortexIntegrationBaseline
        }));
        assert!(
            report.engine_plugin_contracts.iter().all(|plugin| {
                plugin.role != BenchmarkEngineRole::ManagedPlatformDesignReference
            })
        );
    }

    #[test]
    fn result_schema_requires_timing_and_coverage_evidence() {
        let report = plan_benchmark_suite_catalog();
        let schema = report.result_schema_v2;
        let constitution_requirements = report.benchmark_constitution_requirements;

        assert!(schema.timing_scope_required);
        assert!(schema.coverage_status_required);
        assert!(schema.execution_provider_kind_required);
        assert!(schema.certificate_status_required);
        assert!(schema.native_io_status_required);
        assert!(schema.materialization_decode_status_required);
        assert!(schema.fallback_attempted_required);
        assert!(constitution_requirements.covers_rfc_0040_fields());
        assert!(!report.benchmark_constitution_template.fallback_attempted);
    }

    #[test]
    fn coverage_rows_separate_external_baselines_from_shardloom_claims() {
        let report = plan_benchmark_suite_catalog();

        assert!(report.coverage_rows.iter().any(|row| {
            row.engine_id == "shardloom" && row.status == BenchmarkCoverageStatus::Blocked
        }));
        assert!(report.coverage_rows.iter().any(|row| {
            row.engine_id == "vortex_datafusion_integration"
                && row.status == BenchmarkCoverageStatus::ExternalBaselineOnly
                && !row.status.permits_shardloom_claim()
        }));
        assert!(
            report
                .coverage_rows
                .iter()
                .all(|row| !row.fallback_attempted)
        );
    }
}
