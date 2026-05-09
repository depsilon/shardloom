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
    fn human_text_mentions_not_certified_and_fallback_disabled() {
        let text = CapabilityCertificationReport::contract_only().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("best choice claim: not_certified"));
    }
}
