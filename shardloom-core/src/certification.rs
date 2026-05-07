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
            ("iceberg_compatible", "iceberg_compatible"),
            ("delta_compatible", "delta_compatible"),
            ("local_filesystem", "local_filesystem"),
            ("s3_compatible", "s3_compatible"),
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
    fn human_text_mentions_not_certified_and_fallback_disabled() {
        let text = CapabilityCertificationReport::contract_only().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("best choice claim: not_certified"));
    }
}
