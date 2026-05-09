use shardloom_core::{
    AdapterMaturityLevel, CapabilityCertificationReport, CapabilityCertificationStatus,
    FeatureFootprintReport, OperatorCertificationStatus, OperatorMemoryCertification,
    SemanticProfileName, SourcePushdownExactness, SqlCoverageTier,
};

const EXPECTED_SQL_FEATURES: [&str; 23] = [
    "select",
    "with_cte",
    "from_table_or_subquery",
    "where",
    "projection_aliases",
    "group_by",
    "having",
    "order_by",
    "limit_offset",
    "distinct",
    "case_when",
    "casts",
    "scalar_functions",
    "aggregate_functions",
    "window_functions",
    "subqueries",
    "joins",
    "set_operations",
    "create_table_as_select",
    "insert",
    "merge_update_delete",
    "explain",
    "analyze_profile",
];

const EXPECTED_OPERATOR_FAMILIES: [&str; 27] = [
    "scan",
    "filter",
    "project",
    "limit",
    "top_k",
    "sort",
    "aggregate",
    "hash_aggregate",
    "sort_aggregate",
    "window",
    "join",
    "hash_join",
    "sort_merge_join",
    "broadcast_join",
    "semi_join",
    "anti_join",
    "range_join",
    "set_union",
    "set_intersect",
    "set_except",
    "repartition",
    "shuffle_exchange",
    "write",
    "commit",
    "compact",
    "merge",
    "delete",
];

const EXPECTED_FUNCTION_GROUPS: [&str; 32] = [
    "comparison",
    "boolean",
    "math",
    "numeric",
    "decimal",
    "string",
    "regex",
    "binary",
    "date",
    "time",
    "timestamp",
    "interval",
    "timezone",
    "conditional",
    "null_handling",
    "casts",
    "hashing",
    "encoding_aware_predicates",
    "aggregates",
    "approximate_aggregates",
    "statistical_aggregates",
    "window_functions",
    "array_list_functions",
    "struct_functions",
    "map_functions",
    "json_functions",
    "uuid_id_functions",
    "table_functions",
    "metadata_functions",
    "system_introspection_functions",
    "vector_functions",
    "effectful_functions",
];

const EXPECTED_ADAPTERS: [&str; 24] = [
    "native_vortex",
    "parquet",
    "arrow_ipc",
    "csv",
    "jsonl",
    "avro",
    "orc",
    "iceberg_compatible",
    "delta_compatible",
    "hive_partition_discovery",
    "table_snapshot_import_export",
    "schema_evolution_adapter",
    "local_filesystem",
    "s3_compatible",
    "gcs",
    "azure_blob_adls",
    "http_range",
    "local_catalog",
    "hive_compatible_catalog",
    "iceberg_rest_compatible_catalog",
    "glue_like_catalog",
    "nessie_like_catalog",
    "python_api",
    "rust_api",
];

const EXPECTED_SEMANTIC_PROFILES: [&str; 5] = [
    "shardloom_native",
    "spark_compatible",
    "datafusion_compatible",
    "postgres_like",
    "ansi_strict",
];

const EXPECTED_MIGRATION_REPORTS: [&str; 5] = [
    "spark_migration",
    "datafusion_migration",
    "duckdb_polars_migration",
    "sql_compatibility",
    "plan_portability",
];

const EXPECTED_SCORECARD_DIMENSIONS: [&str; 13] = [
    "correctness",
    "performance",
    "cost",
    "memory_safety",
    "sql_coverage",
    "function_coverage",
    "operator_coverage",
    "adapter_coverage",
    "api_usability",
    "observability",
    "migration_ease",
    "deployment_ease",
    "no_fallback_integrity",
];

#[test]
fn capability_certification_matrix_names_are_stable() {
    let report = CapabilityCertificationReport::contract_only();

    assert_eq!(sql_features(&report).as_slice(), EXPECTED_SQL_FEATURES);
    assert_eq!(
        operator_families(&report).as_slice(),
        EXPECTED_OPERATOR_FAMILIES
    );
    assert_eq!(
        function_groups(&report).as_slice(),
        EXPECTED_FUNCTION_GROUPS
    );
    assert_eq!(adapter_ids(&report).as_slice(), EXPECTED_ADAPTERS);
    assert_eq!(
        semantic_profiles(&report).as_slice(),
        EXPECTED_SEMANTIC_PROFILES
    );
    assert_eq!(
        migration_reports(&report).as_slice(),
        EXPECTED_MIGRATION_REPORTS
    );
    assert_eq!(
        scorecard_dimensions(&report).as_slice(),
        EXPECTED_SCORECARD_DIMENSIONS
    );
}

#[test]
fn capability_certification_schema_versions_are_stable() {
    let report = CapabilityCertificationReport::contract_only();

    assert_eq!(
        report.schema_version,
        "shardloom.capability_certification.v1"
    );
    assert_eq!(
        report.sql_coverage.schema_version,
        "shardloom.sql_coverage.v1"
    );
    assert_eq!(
        report.operator_coverage.schema_version,
        "shardloom.operator_coverage.v1"
    );
    assert_eq!(
        report.function_coverage.schema_version,
        "shardloom.function_coverage.v1"
    );
    assert_eq!(
        report.adapter_certification.schema_version,
        "shardloom.adapter_certification.v1"
    );
    assert_eq!(
        report.best_choice_scorecard.schema_version,
        "shardloom.best_choice_scorecard.v1"
    );
}

#[test]
fn planned_capability_certification_defaults_are_not_supported() {
    let report = CapabilityCertificationReport::contract_only();

    assert!(
        report
            .sql_coverage
            .entries
            .iter()
            .all(planned_sql_entry_is_not_supported)
    );
    assert!(
        report
            .operator_coverage
            .entries
            .iter()
            .all(planned_operator_entry_is_not_supported)
    );
    assert!(
        report
            .function_coverage
            .entries
            .iter()
            .all(planned_function_entry_is_not_supported)
    );
    assert!(
        report
            .adapter_certification
            .entries
            .iter()
            .all(planned_adapter_entry_is_not_supported)
    );
    assert!(
        report
            .semantic_profiles
            .iter()
            .all(planned_semantic_profile_is_not_supported)
    );
    assert!(
        report
            .migration_reports
            .iter()
            .all(planned_migration_report_is_not_supported)
    );
    assert!(report.best_choice_scorecard.dimensions.iter().all(|entry| {
        entry.status == CapabilityCertificationStatus::Planned
            && entry.evidence_label == "not_certified"
            && !entry.fallback_attempted
    }));
    assert!(!report.can_publish_best_choice_claim());
}

#[test]
fn certification_and_feature_footprint_share_no_probe_contract() {
    let certification = CapabilityCertificationReport::contract_only();
    let footprint = FeatureFootprintReport::contract_only();

    assert_eq!(certification.engine_version, footprint.engine_version);
    assert!(!certification.fallback_attempted());
    assert!(!footprint.fallback_execution_allowed());
    assert!(certification.diagnostics.is_empty());
    assert!(footprint.diagnostics.is_empty());
    assert!(!certification.to_human_text().contains("generated_at"));
    assert!(!footprint.to_human_text().contains("generated_at"));
}

fn sql_features(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .sql_coverage
        .entries
        .iter()
        .map(|entry| entry.feature.as_str())
        .collect()
}

fn operator_families(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .operator_coverage
        .entries
        .iter()
        .map(|entry| entry.family.as_str())
        .collect()
}

fn function_groups(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .function_coverage
        .entries
        .iter()
        .map(|entry| entry.group.as_str())
        .collect()
}

fn adapter_ids(report: &CapabilityCertificationReport) -> Vec<&str> {
    report
        .adapter_certification
        .entries
        .iter()
        .map(|entry| entry.adapter_id.as_str())
        .collect()
}

fn semantic_profiles(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .semantic_profiles
        .iter()
        .map(|entry| entry.profile.as_str())
        .collect()
}

fn migration_reports(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .migration_reports
        .iter()
        .map(|entry| entry.report_kind.as_str())
        .collect()
}

fn scorecard_dimensions(report: &CapabilityCertificationReport) -> Vec<&'static str> {
    report
        .best_choice_scorecard
        .dimensions
        .iter()
        .map(|entry| entry.dimension.as_str())
        .collect()
}

fn planned_sql_entry_is_not_supported(entry: &shardloom_core::SqlCoverageEntry) -> bool {
    entry.status == CapabilityCertificationStatus::Planned
        && entry.tier == SqlCoverageTier::Unsupported
        && entry.semantic_profile == SemanticProfileName::ShardLoomNative
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
        && !entry.can_satisfy_production_claim()
}

fn planned_operator_entry_is_not_supported(entry: &shardloom_core::OperatorCoverageEntry) -> bool {
    entry.status == OperatorCertificationStatus::Planned
        && entry.memory == OperatorMemoryCertification::unsupported()
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
        && !entry.status.can_satisfy_production_claim()
}

fn planned_function_entry_is_not_supported(entry: &shardloom_core::FunctionCoverageEntry) -> bool {
    entry.status == CapabilityCertificationStatus::Planned
        && !entry.encoded_capable
        && !entry.selection_vector_supported
        && !entry.streaming_supported
        && !entry.spill_supported
        && !entry.materialization_required
        && entry.semantic_profile == SemanticProfileName::ShardLoomNative
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
}

fn planned_adapter_entry_is_not_supported(
    entry: &shardloom_core::AdapterCertificationEntry,
) -> bool {
    entry.status == CapabilityCertificationStatus::Planned
        && entry.maturity == AdapterMaturityLevel::DeclaredOnly
        && entry.pushdown_exactness == SourcePushdownExactness::Unsupported
        && !entry.encoded_representation_preserved
        && !entry.materialization_required
        && !entry.read_supported
        && !entry.write_supported
        && !entry.commit_supported
        && !entry.streaming_supported
        && !entry.object_store_range_supported
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
}

fn planned_semantic_profile_is_not_supported(entry: &shardloom_core::SemanticProfileEntry) -> bool {
    entry.status == CapabilityCertificationStatus::Planned
        && !entry.dimensions_declared
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
}

fn planned_migration_report_is_not_supported(
    entry: &shardloom_core::MigrationCompatibilityEntry,
) -> bool {
    entry.status == CapabilityCertificationStatus::Planned
        && entry.supported_constructs.is_empty()
        && entry.unsupported_constructs.is_empty()
        && entry.semantic_differences.is_empty()
        && entry.rewrite_suggestions.is_empty()
        && entry.performance_cost_delta_estimate.is_none()
        && entry.vortex_conversion_payback_estimate.is_none()
        && !entry.fallback_attempted
        && entry.diagnostics.is_empty()
}
