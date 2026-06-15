//! Extension, plugin ABI, and sandboxing domain skeleton.
//!
//! This module is metadata-only. It does not load plugins, execute extension code,
//! inspect external files, or enable fallback execution.
#![allow(
    clippy::missing_errors_doc,
    clippy::struct_excessive_bools,
    clippy::semicolon_if_nothing_returned
)]

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, EffectLevel, ExternalEffectKind,
    PermissionKind, Result, ShardLoomError,
};

fn validate_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must not be empty"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExtensionId(String);
impl ExtensionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("extension id", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<String>,
}
impl ExtensionVersion {
    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            pre_release: None,
        }
    }
    pub fn with_pre_release(mut self, pre_release: impl Into<String>) -> Result<Self> {
        let v = pre_release.into();
        validate_non_empty("pre-release", &v)?;
        self.pre_release = Some(v);
        Ok(self)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match &self.pre_release {
            Some(pr) => format!("{}.{}.{}-{}", self.major, self.minor, self.patch, pr),
            None => format!("{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionCategory {
    Frontend,
    Function,
    ScalarUdf,
    AggregateUdf,
    TableFunction,
    EncodedKernel,
    TranslationSink,
    Connector,
    CatalogProvider,
    ObjectStoreProvider,
    EffectProvider,
    LlmProvider,
    EmbeddingProvider,
    VectorIndexProvider,
    ObservabilityExporter,
    BenchmarkProvider,
    Unknown,
}
impl ExtensionCategory {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::Function => "function",
            Self::ScalarUdf => "scalar_udf",
            Self::AggregateUdf => "aggregate_udf",
            Self::TableFunction => "table_function",
            Self::EncodedKernel => "encoded_kernel",
            Self::TranslationSink => "translation_sink",
            Self::Connector => "connector",
            Self::CatalogProvider => "catalog_provider",
            Self::ObjectStoreProvider => "object_store_provider",
            Self::EffectProvider => "effect_provider",
            Self::LlmProvider => "llm_provider",
            Self::EmbeddingProvider => "embedding_provider",
            Self::VectorIndexProvider => "vector_index_provider",
            Self::ObservabilityExporter => "observability_exporter",
            Self::BenchmarkProvider => "benchmark_provider",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_effect_provider(&self) -> bool {
        matches!(
            self,
            Self::EffectProvider
                | Self::LlmProvider
                | Self::EmbeddingProvider
                | Self::VectorIndexProvider
        )
    }
    #[must_use]
    pub const fn is_execution_related(&self) -> bool {
        matches!(
            self,
            Self::Function
                | Self::ScalarUdf
                | Self::AggregateUdf
                | Self::TableFunction
                | Self::EncodedKernel
                | Self::TranslationSink
                | Self::Connector
                | Self::CatalogProvider
                | Self::ObjectStoreProvider
                | Self::EffectProvider
                | Self::LlmProvider
                | Self::EmbeddingProvider
                | Self::VectorIndexProvider
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionLifecycleState {
    Discovered,
    Loaded,
    Validated,
    Enabled,
    Disabled,
    Failed,
    Quarantined,
    Deprecated,
    Removed,
    Unsupported,
}
impl ExtensionLifecycleState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Discovered => "discovered",
            Self::Loaded => "loaded",
            Self::Validated => "validated",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Failed => "failed",
            Self::Quarantined => "quarantined",
            Self::Deprecated => "deprecated",
            Self::Removed => "removed",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Enabled | Self::Validated)
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::Failed | Self::Quarantined | Self::Removed | Self::Unsupported
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionCapabilityStatus {
    Supported,
    PartiallySupported,
    Planned,
    Disabled,
    RequiresConfiguration,
    RequiresExplicitEnablement,
    Unsupported,
}
impl ExtensionCapabilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::PartiallySupported => "partially_supported",
            Self::Planned => "planned",
            Self::Disabled => "disabled",
            Self::RequiresConfiguration => "requires_configuration",
            Self::RequiresExplicitEnablement => "requires_explicit_enablement",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Supported | Self::PartiallySupported)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionCapability {
    pub name: String,
    pub status: ExtensionCapabilityStatus,
    pub notes: Option<String>,
}
impl ExtensionCapability {
    pub fn new(name: impl Into<String>, status: ExtensionCapabilityStatus) -> Result<Self> {
        let n = name.into();
        validate_non_empty("capability name", &n)?;
        Ok(Self {
            name: n,
            status,
            notes: None,
        })
    }
    #[must_use]
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        self.status.is_usable()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}:{}", self.name, self.status.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginAbiStatus {
    InternalOnly,
    Experimental,
    Compatible,
    Incompatible,
    NotChecked,
    Unsupported,
}
impl PluginAbiStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InternalOnly => "internal_only",
            Self::Experimental => "experimental",
            Self::Compatible => "compatible",
            Self::Incompatible => "incompatible",
            Self::NotChecked => "not_checked",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn allows_loading(&self) -> bool {
        matches!(self, Self::Compatible)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAbiRequirement {
    pub api_name: String,
    pub required_version: ExtensionVersion,
    pub status: PluginAbiStatus,
}
impl PluginAbiRequirement {
    pub fn new(api_name: impl Into<String>, required_version: ExtensionVersion) -> Result<Self> {
        let n = api_name.into();
        validate_non_empty("api name", &n)?;
        Ok(Self {
            api_name: n,
            required_version,
            status: PluginAbiStatus::NotChecked,
        })
    }
    #[must_use]
    pub fn with_status(mut self, status: PluginAbiStatus) -> Self {
        self.status = status;
        self
    }
    #[must_use]
    pub const fn allows_loading(&self) -> bool {
        self.status.allows_loading()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} {} ({})",
            self.api_name,
            self.required_version.summary(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfRuntimeKind {
    BuiltinDeterministicFixture,
    RustNative,
    Wasm,
    Python,
    SqlDefined,
    ExternalService,
    Unknown,
}
impl UdfRuntimeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BuiltinDeterministicFixture => "builtin_deterministic_fixture",
            Self::RustNative => "rust_native",
            Self::Wasm => "wasm",
            Self::Python => "python",
            Self::SqlDefined => "sql_defined",
            Self::ExternalService => "external_service",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_sandboxing(&self) -> bool {
        matches!(
            self,
            Self::RustNative | Self::Wasm | Self::Python | Self::ExternalService
        )
    }
    #[must_use]
    pub const fn is_available_initially(&self) -> bool {
        matches!(self, Self::BuiltinDeterministicFixture)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedUdfKind {
    Scalar,
    Aggregate,
    TableFunction,
}

impl TypedUdfKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
            Self::TableFunction => "table_function",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedUdfRegistryStatus {
    AdmittedLocalFixture,
    BlockedMissingRuntimeBridge,
    BlockedSandboxPolicy,
    BlockedMaterializationBoundary,
}

impl TypedUdfRegistryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AdmittedLocalFixture => "admitted_local_fixture",
            Self::BlockedMissingRuntimeBridge => "blocked_missing_runtime_bridge",
            Self::BlockedSandboxPolicy => "blocked_sandbox_policy",
            Self::BlockedMaterializationBoundary => "blocked_materialization_boundary",
        }
    }

    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        matches!(self, Self::AdmittedLocalFixture)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedUdfEncodedCapability {
    EncodedNativeCandidate,
    LateMaterializedFixture,
    MaterializationRequired,
    Unsupported,
}

impl TypedUdfEncodedCapability {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EncodedNativeCandidate => "encoded_native_candidate",
            Self::LateMaterializedFixture => "late_materialized_fixture",
            Self::MaterializationRequired => "materialization_required",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn requires_materialization(&self) -> bool {
        matches!(self, Self::MaterializationRequired)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedUdfRegistryEntry {
    pub udf_id: &'static str,
    pub display_name: &'static str,
    pub udf_version: &'static str,
    pub kind: TypedUdfKind,
    pub runtime_kind: UdfRuntimeKind,
    pub support_status: TypedUdfRegistryStatus,
    pub encoded_capability: TypedUdfEncodedCapability,
    pub determinism: ExtensionDeterminismContract,
    pub null_behavior: ExtensionNullBehaviorContract,
    pub materialization: ExtensionMaterializationContract,
    pub input_dtypes: &'static [&'static str],
    pub output_dtype: &'static str,
    pub sandbox_policy: SandboxPolicyKind,
    pub permission_contract: &'static str,
    pub effect_level: EffectLevel,
    pub runtime_fixture_command: Option<&'static str>,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub registry_execution_allowed: bool,
    pub runtime_fixture_available: bool,
    pub sandbox_required: bool,
    pub filesystem_access_allowed: bool,
    pub network_access_allowed: bool,
    pub secret_access_allowed: bool,
    pub credential_resolution_required: bool,
    pub dynamic_loading_allowed: bool,
    pub runtime_execution_performed: bool,
    pub extension_code_executed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl TypedUdfRegistryEntry {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        udf_id: &'static str,
        display_name: &'static str,
        udf_version: &'static str,
        kind: TypedUdfKind,
        runtime_kind: UdfRuntimeKind,
        support_status: TypedUdfRegistryStatus,
        encoded_capability: TypedUdfEncodedCapability,
        determinism: ExtensionDeterminismContract,
        null_behavior: ExtensionNullBehaviorContract,
        materialization: ExtensionMaterializationContract,
        input_dtypes: &'static [&'static str],
        output_dtype: &'static str,
        sandbox_policy: SandboxPolicyKind,
        permission_contract: &'static str,
        effect_level: EffectLevel,
        runtime_fixture_command: Option<&'static str>,
        blocker_id: &'static str,
        diagnostic_code: &'static str,
        required_evidence: &'static str,
        registry_execution_allowed: bool,
        runtime_fixture_available: bool,
        sandbox_required: bool,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            udf_id,
            display_name,
            udf_version,
            kind,
            runtime_kind,
            support_status,
            encoded_capability,
            determinism,
            null_behavior,
            materialization,
            input_dtypes,
            output_dtype,
            sandbox_policy,
            permission_contract,
            effect_level,
            runtime_fixture_command,
            blocker_id,
            diagnostic_code,
            required_evidence,
            registry_execution_allowed,
            runtime_fixture_available,
            sandbox_required,
            filesystem_access_allowed: false,
            network_access_allowed: false,
            secret_access_allowed: false,
            credential_resolution_required: false,
            dynamic_loading_allowed: false,
            runtime_execution_performed: false,
            extension_code_executed: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }

    #[must_use]
    pub fn input_dtype_summary(&self) -> String {
        if self.input_dtypes.is_empty() {
            return "none".to_string();
        }
        self.input_dtypes.join(",")
    }

    #[must_use]
    pub const fn materialization_required(&self) -> bool {
        self.encoded_capability.requires_materialization()
            || self.materialization.requires_materialization()
    }

    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        self.support_status.is_admitted()
    }

    #[must_use]
    pub const fn no_fallback_invariant_holds(&self) -> bool {
        !self.filesystem_access_allowed
            && !self.network_access_allowed
            && !self.secret_access_allowed
            && !self.credential_resolution_required
            && !self.dynamic_loading_allowed
            && !self.runtime_execution_performed
            && !self.extension_code_executed
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedUdfRegistryReport {
    pub schema_version: &'static str,
    pub registry_id: &'static str,
    pub docs_ref: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub entries: Vec<TypedUdfRegistryEntry>,
    pub local_fixture_execution_bridge_available: bool,
    pub arbitrary_runtime_bridge_available: bool,
    pub sandbox_policy_declared: bool,
    pub filesystem_access_allowed: bool,
    pub network_access_allowed: bool,
    pub secret_access_allowed: bool,
    pub dynamic_loading_allowed: bool,
    pub runtime_execution_performed: bool,
    pub extension_code_executed: bool,
    pub external_effect_executed: bool,
    pub credential_resolution_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

fn typed_udf_registry_entries() -> Vec<TypedUdfRegistryEntry> {
    vec![
        typed_udf_fixture_entry(),
        typed_udf_native_aggregate_candidate_entry(),
        typed_udf_table_function_boundary_entry(),
        typed_udf_python_boundary_entry(),
    ]
}

const fn typed_udf_fixture_entry() -> TypedUdfRegistryEntry {
    TypedUdfRegistryEntry::new(
        "sl_fixture_double_i64",
        "Built-in nullable int64 double fixture",
        "0.1.0",
        TypedUdfKind::Scalar,
        UdfRuntimeKind::BuiltinDeterministicFixture,
        TypedUdfRegistryStatus::AdmittedLocalFixture,
        TypedUdfEncodedCapability::LateMaterializedFixture,
        ExtensionDeterminismContract::PureDeterministic,
        ExtensionNullBehaviorContract::NullPropagating,
        ExtensionMaterializationContract::LateMaterialized,
        &["int64_nullable"],
        "int64_nullable",
        SandboxPolicyKind::None,
        "none_builtin_pure_fixture",
        EffectLevel::PureDeterministic,
        Some("udf-local-scalar-fixture-smoke"),
        "none_builtin_fixture",
        "SL_UDF_FIXTURE_ADMITTED",
        "typed_registry_entry,nullable_int64_contract,overflow_check,no_fallback_evidence",
        true,
        true,
        false,
        "Only this built-in deterministic scalar fixture is admitted. It does not load extension code, access files, access secrets, use the network, or invoke a fallback engine.",
    )
}

const fn typed_udf_native_aggregate_candidate_entry() -> TypedUdfRegistryEntry {
    TypedUdfRegistryEntry::new(
        "sl_native_sum_i64",
        "Scoped native aggregate UDF candidate",
        "0.0.0",
        TypedUdfKind::Aggregate,
        UdfRuntimeKind::RustNative,
        TypedUdfRegistryStatus::BlockedMissingRuntimeBridge,
        TypedUdfEncodedCapability::EncodedNativeCandidate,
        ExtensionDeterminismContract::PureDeterministic,
        ExtensionNullBehaviorContract::NullSkipping,
        ExtensionMaterializationContract::EncodedNative,
        &["int64_nullable"],
        "int64_nullable",
        SandboxPolicyKind::FullSandboxRequired,
        "execute_udf_required_before_runtime",
        EffectLevel::PureDeterministic,
        None,
        "prod-ready-1f.aggregate_udf_runtime_bridge_blocked",
        "SL_UDF_RUNTIME_BLOCKED",
        "aggregate_state_contract,encoded_kernel,spill_policy,sandbox_policy,execution_certificate,no_fallback_evidence",
        false,
        false,
        true,
        "Aggregate UDF registry metadata is declared, but execution is blocked until aggregate state, encoded kernels, spill, sandbox, and certificate evidence exist.",
    )
}

const fn typed_udf_table_function_boundary_entry() -> TypedUdfRegistryEntry {
    TypedUdfRegistryEntry::new(
        "sl_table_generate_series_i64",
        "Scoped table-function UDF candidate",
        "0.0.0",
        TypedUdfKind::TableFunction,
        UdfRuntimeKind::SqlDefined,
        TypedUdfRegistryStatus::BlockedMaterializationBoundary,
        TypedUdfEncodedCapability::MaterializationRequired,
        ExtensionDeterminismContract::PureDeterministic,
        ExtensionNullBehaviorContract::NullError,
        ExtensionMaterializationContract::MaterializationRequired,
        &["int64", "int64"],
        "table<int64>",
        SandboxPolicyKind::FullSandboxRequired,
        "table_function_source_sink_policy_required",
        EffectLevel::PureDeterministic,
        None,
        "prod-ready-1f.table_function_materialization_blocked",
        "SL_UDF_MATERIALIZATION_BLOCKED",
        "table_function_contract,source_sink_policy,materialization_boundary,execution_certificate,no_fallback_evidence",
        false,
        false,
        true,
        "Table-function UDF metadata is declared, but execution is blocked until source/sink and materialization boundaries are certified.",
    )
}

const fn typed_udf_python_boundary_entry() -> TypedUdfRegistryEntry {
    TypedUdfRegistryEntry::new(
        "external_python_scalar_boundary",
        "Python scalar UDF boundary",
        "0.0.0",
        TypedUdfKind::Scalar,
        UdfRuntimeKind::Python,
        TypedUdfRegistryStatus::BlockedSandboxPolicy,
        TypedUdfEncodedCapability::MaterializationRequired,
        ExtensionDeterminismContract::Unknown,
        ExtensionNullBehaviorContract::Unknown,
        ExtensionMaterializationContract::MaterializationRequired,
        &["declared_by_manifest"],
        "declared_by_manifest",
        SandboxPolicyKind::FullSandboxRequired,
        "python_materialization_effect_policy_required",
        EffectLevel::Unknown,
        None,
        "prod-ready-1f.python_udf_sandbox_blocked",
        "SL_UDF_SANDBOX_BLOCKED",
        "python_boundary,materialization_policy,redaction_policy,sandbox_policy,timeout_memory_cpu_policy,execution_certificate,no_fallback_evidence",
        false,
        false,
        true,
        "Python UDFs remain blocked and must be explicit materialization/effect boundaries; no interpreter bridge, callable execution, network, credentials, or fallback execution is enabled.",
    )
}

impl TypedUdfRegistryReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.typed_udf_registry.v1",
            registry_id: "prod-ready-1f.typed_udf_registry",
            docs_ref: "docs/architecture/udf-external-effect-blocker-matrix.md",
            support_status: "scoped_fixture_supported",
            claim_gate_status: "fixture_smoke_only",
            entries: typed_udf_registry_entries(),
            local_fixture_execution_bridge_available: true,
            arbitrary_runtime_bridge_available: false,
            sandbox_policy_declared: true,
            filesystem_access_allowed: false,
            network_access_allowed: false,
            secret_access_allowed: false,
            dynamic_loading_allowed: false,
            runtime_execution_performed: false,
            extension_code_executed: false,
            external_effect_executed: false,
            credential_resolution_performed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.entries.iter().map(|entry| entry.udf_id).collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.entries.iter().map(|entry| entry.blocker_id).collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.required_evidence)
            .collect()
    }

    #[must_use]
    pub fn admitted_local_fixture_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.is_admitted())
            .count()
    }

    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| !entry.is_admitted())
            .count()
    }

    #[must_use]
    pub fn scalar_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.kind == TypedUdfKind::Scalar)
            .count()
    }

    #[must_use]
    pub fn aggregate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.kind == TypedUdfKind::Aggregate)
            .count()
    }

    #[must_use]
    pub fn table_function_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.kind == TypedUdfKind::TableFunction)
            .count()
    }

    #[must_use]
    pub fn encoded_native_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                entry.encoded_capability == TypedUdfEncodedCapability::EncodedNativeCandidate
            })
            .count()
    }

    #[must_use]
    pub fn materialization_required_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.materialization_required())
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.filesystem_access_allowed
            && !self.network_access_allowed
            && !self.secret_access_allowed
            && !self.dynamic_loading_allowed
            && !self.runtime_execution_performed
            && !self.extension_code_executed
            && !self.external_effect_executed
            && !self.credential_resolution_performed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .entries
                .iter()
                .all(TypedUdfRegistryEntry::no_fallback_invariant_holds)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "typed UDF registry entries={} admitted_fixtures={} blocked={} scalar={} aggregate={} table_function={} fallback_execution=disabled",
            self.entries.len(),
            self.admitted_local_fixture_count(),
            self.blocked_count(),
            self.scalar_count(),
            self.aggregate_count(),
            self.table_function_count()
        )
    }
}

#[must_use]
pub fn typed_udf_registry_report() -> TypedUdfRegistryReport {
    TypedUdfRegistryReport::current()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicScalarUdfFixtureReport {
    pub schema_version: &'static str,
    pub udf_id: &'static str,
    pub udf_version: &'static str,
    pub runtime_kind: UdfRuntimeKind,
    pub input_dtype: &'static str,
    pub output_dtype: &'static str,
    pub determinism: &'static str,
    pub null_policy: &'static str,
    pub input_row_count: usize,
    pub output_row_count: usize,
    pub input_digest: String,
    pub output_digest: String,
    pub output_values: Vec<Option<i64>>,
    pub overflow_policy_enforced: bool,
    pub overflow_blocked: bool,
    pub sandbox_required: bool,
    pub network_allowed: bool,
    pub credential_resolution_performed: bool,
    pub dynamic_loading_performed: bool,
    pub extension_code_executed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
}

impl DeterministicScalarUdfFixtureReport {
    #[must_use]
    pub fn output_values_summary(&self) -> String {
        self.output_values
            .iter()
            .map(|value| value.map_or_else(|| "null".to_string(), |v| v.to_string()))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.sandbox_required
            && !self.network_allowed
            && !self.credential_resolution_performed
            && !self.dynamic_loading_performed
            && !self.extension_code_executed
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "deterministic scalar UDF fixture\nudf: {} {}\nruntime: {}\ninput rows: {}\noutput rows: {}\noutputs: {}\nfallback execution: disabled",
            self.udf_id,
            self.udf_version,
            self.runtime_kind.as_str(),
            self.input_row_count,
            self.output_row_count,
            self.output_values_summary()
        )
    }
}

/// Execute the only admitted UDF fixture: a built-in, pure, null-propagating
/// `i64 -> i64` scalar that doubles the input value with overflow blocking.
///
/// This is intentionally not an arbitrary UDF registry, plugin loader, WASM
/// runtime, Python bridge, SQL-defined UDF, or external-service UDF.
///
/// # Errors
/// Returns an explicit invalid-operation error if an input would overflow.
pub fn run_deterministic_scalar_udf_fixture(
    values: &[Option<i64>],
) -> Result<DeterministicScalarUdfFixtureReport> {
    let mut output_values = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Some(v) => output_values.push(Some(v.checked_mul(2).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "deterministic scalar UDF fixture overflow blocked; no fallback execution was attempted"
                        .to_string(),
                )
            })?)),
            None => output_values.push(None),
        }
    }
    let input_summary = summarize_optional_i64_values(values);
    let output_summary = summarize_optional_i64_values(&output_values);
    Ok(DeterministicScalarUdfFixtureReport {
        schema_version: "shardloom.deterministic_scalar_udf_fixture.v1",
        udf_id: "sl_fixture_double_i64",
        udf_version: "0.1.0",
        runtime_kind: UdfRuntimeKind::BuiltinDeterministicFixture,
        input_dtype: "int64_nullable",
        output_dtype: "int64_nullable",
        determinism: "pure_deterministic",
        null_policy: "null_propagating",
        input_row_count: values.len(),
        output_row_count: output_values.len(),
        input_digest: fnv64_digest_text(&input_summary),
        output_digest: fnv64_digest_text(&output_summary),
        output_values,
        overflow_policy_enforced: true,
        overflow_blocked: false,
        sandbox_required: false,
        network_allowed: false,
        credential_resolution_performed: false,
        dynamic_loading_performed: false,
        extension_code_executed: false,
        external_effect_executed: false,
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "fixture_smoke_only",
        claim_boundary: "Only the built-in deterministic scalar UDF fixture is admitted; arbitrary Rust, WASM, Python, SQL-defined, table-function, and external-service UDF execution remains blocked.",
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicEmbeddingVectorFixtureReport {
    pub schema_version: &'static str,
    pub fixture_id: &'static str,
    pub fixture_version: &'static str,
    pub input_dtype: &'static str,
    pub output_dtype: &'static str,
    pub determinism: &'static str,
    pub embedding_model_id: &'static str,
    pub vector_index_kind: &'static str,
    pub metric: &'static str,
    pub dimension: usize,
    pub input_row_count: usize,
    pub vector_row_count: usize,
    pub query_text: String,
    pub query_vector: [i64; 4],
    pub nearest_index: usize,
    pub nearest_text: String,
    pub nearest_distance_squared: i64,
    pub input_digest: String,
    pub vector_digest: String,
    pub model_call_performed: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dynamic_loading_performed: bool,
    pub extension_code_executed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
}

impl DeterministicEmbeddingVectorFixtureReport {
    #[must_use]
    pub fn query_vector_summary(&self) -> String {
        summarize_vector(&self.query_vector)
    }

    #[must_use]
    pub fn nearest_summary(&self) -> String {
        format!(
            "{}:{}:{}",
            self.nearest_index, self.nearest_text, self.nearest_distance_squared
        )
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.model_call_performed
            && !self.credential_resolution_performed
            && !self.network_probe_performed
            && !self.dynamic_loading_performed
            && !self.extension_code_executed
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "deterministic embedding/vector fixture\nfixture: {} {}\nmodel: {}\nmetric: {}\ninput rows: {}\nnearest: {}\nfallback execution: disabled",
            self.fixture_id,
            self.fixture_version,
            self.embedding_model_id,
            self.metric,
            self.input_row_count,
            self.nearest_summary()
        )
    }
}

/// Execute the only admitted embedding/vector fixture: a built-in deterministic
/// text-to-vector transform plus local brute-force nearest-neighbor proof.
///
/// This is intentionally not an embedding model call, vector database, network
/// search service, plugin, external API call, or fallback execution path.
///
/// # Errors
/// Returns an explicit invalid-operation error when no non-empty fixture texts
/// are supplied.
pub fn run_deterministic_embedding_vector_fixture(
    texts: &[String],
    query_text: &str,
) -> Result<DeterministicEmbeddingVectorFixtureReport> {
    if texts.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "embedding/vector fixture requires at least one input text".to_string(),
        ));
    }
    if texts.iter().any(|value| value.trim().is_empty()) {
        return Err(ShardLoomError::InvalidOperation(
            "embedding/vector fixture input texts must not be empty".to_string(),
        ));
    }
    if query_text.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "embedding/vector fixture query text must not be empty".to_string(),
        ));
    }

    let vectors = texts
        .iter()
        .map(|text| deterministic_fixture_embedding(text))
        .collect::<Vec<_>>();
    let query_vector = deterministic_fixture_embedding(query_text);
    let Some((nearest_index, nearest_distance_squared)) = vectors
        .iter()
        .enumerate()
        .map(|(index, vector)| (index, squared_vector_distance(&query_vector, vector)))
        .min_by_key(|(index, distance)| (*distance, *index))
    else {
        return Err(ShardLoomError::InvalidOperation(
            "embedding/vector fixture requires at least one candidate vector".to_string(),
        ));
    };
    let nearest_text = texts[nearest_index].clone();
    let input_summary = texts.join("\u{1f}");
    let vector_summary = summarize_vectors(&vectors);
    Ok(DeterministicEmbeddingVectorFixtureReport {
        schema_version: "shardloom.deterministic_embedding_vector_fixture.v1",
        fixture_id: "sl_fixture_hash_embedding_vector",
        fixture_version: "0.1.0",
        input_dtype: "utf8",
        output_dtype: "fixed_size_list<int64,4>",
        determinism: "pure_deterministic",
        embedding_model_id: "sl_fixture_hash_embedding_v1",
        vector_index_kind: "local_bruteforce_l2_fixture",
        metric: "squared_l2",
        dimension: 4,
        input_row_count: texts.len(),
        vector_row_count: vectors.len(),
        query_text: query_text.to_string(),
        query_vector,
        nearest_index,
        nearest_text,
        nearest_distance_squared,
        input_digest: fnv64_digest_text(&input_summary),
        vector_digest: fnv64_digest_text(&vector_summary),
        model_call_performed: false,
        credential_resolution_performed: false,
        network_probe_performed: false,
        dynamic_loading_performed: false,
        extension_code_executed: false,
        external_effect_executed: false,
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "fixture_smoke_only",
        claim_boundary: "Only the built-in deterministic embedding/vector fixture is admitted; real model calls, embedding generation, vector databases, ANN indexes, external APIs, credentials, network effects, and fallback execution remain blocked.",
    })
}

fn summarize_optional_i64_values(values: &[Option<i64>]) -> String {
    values
        .iter()
        .map(|value| value.map_or_else(|| "null".to_string(), |v| v.to_string()))
        .collect::<Vec<_>>()
        .join(",")
}

fn deterministic_fixture_embedding(text: &str) -> [i64; 4] {
    let bytes = text.as_bytes();
    let len = i64::try_from(text.chars().count()).unwrap_or(i64::MAX);
    let byte_sum = bytes.iter().map(|byte| i64::from(*byte)).sum::<i64>();
    let vowel_count = i64::try_from(
        text.chars()
            .filter(|ch| matches!(ch.to_ascii_lowercase(), 'a' | 'e' | 'i' | 'o' | 'u'))
            .count(),
    )
    .unwrap_or(i64::MAX);
    let hash_bucket = i64::try_from(fnv64_numeric(text) % 10_000).unwrap_or(0);
    [len, byte_sum, vowel_count, hash_bucket]
}

fn squared_vector_distance(left: &[i64; 4], right: &[i64; 4]) -> i64 {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| {
            let delta = l - r;
            delta * delta
        })
        .sum()
}

fn summarize_vector(vector: &[i64; 4]) -> String {
    format!("[{},{},{},{}]", vector[0], vector[1], vector[2], vector[3])
}

fn summarize_vectors(vectors: &[[i64; 4]]) -> String {
    vectors
        .iter()
        .map(summarize_vector)
        .collect::<Vec<_>>()
        .join(";")
}

fn fnv64_digest_text(value: &str) -> String {
    format!("fnv64:{:016x}", fnv64_numeric(value))
}

fn fnv64_numeric(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPolicyKind {
    None,
    MetadataOnly,
    NoNetwork,
    NoFilesystem,
    BoundedResources,
    FullSandboxRequired,
    Unsupported,
}
impl SandboxPolicyKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::MetadataOnly => "metadata_only",
            Self::NoNetwork => "no_network",
            Self::NoFilesystem => "no_filesystem",
            Self::BoundedResources => "bounded_resources",
            Self::FullSandboxRequired => "full_sandbox_required",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_restrictive(&self) -> bool {
        !matches!(self, Self::None)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPolicy {
    pub kind: SandboxPolicyKind,
    pub allow_filesystem: bool,
    pub allow_network: bool,
    pub allow_environment: bool,
    pub allow_secret_access: bool,
    pub max_memory_bytes: Option<u64>,
    pub max_runtime_millis: Option<u64>,
}
impl SandboxPolicy {
    #[must_use]
    pub const fn metadata_only() -> Self {
        Self {
            kind: SandboxPolicyKind::MetadataOnly,
            allow_filesystem: false,
            allow_network: false,
            allow_environment: false,
            allow_secret_access: false,
            max_memory_bytes: None,
            max_runtime_millis: None,
        }
    }
    #[must_use]
    pub const fn full_sandbox_required() -> Self {
        Self {
            kind: SandboxPolicyKind::FullSandboxRequired,
            allow_filesystem: false,
            allow_network: false,
            allow_environment: false,
            allow_secret_access: false,
            max_memory_bytes: None,
            max_runtime_millis: None,
        }
    }
    #[must_use]
    pub const fn allow_filesystem(mut self, value: bool) -> Self {
        self.allow_filesystem = value;
        self
    }
    #[must_use]
    pub const fn allow_network(mut self, value: bool) -> Self {
        self.allow_network = value;
        self
    }
    #[must_use]
    pub const fn allow_environment(mut self, value: bool) -> Self {
        self.allow_environment = value;
        self
    }
    #[must_use]
    pub const fn allow_secret_access(mut self, value: bool) -> Self {
        self.allow_secret_access = value;
        self
    }
    #[must_use]
    pub const fn with_max_memory_bytes(mut self, value: u64) -> Self {
        self.max_memory_bytes = Some(value);
        self
    }
    #[must_use]
    pub const fn with_max_runtime_millis(mut self, value: u64) -> Self {
        self.max_runtime_millis = Some(value);
        self
    }
    #[must_use]
    pub const fn is_safe_default(&self) -> bool {
        !self.allow_filesystem
            && !self.allow_network
            && !self.allow_environment
            && !self.allow_secret_access
    }
    #[must_use]
    pub const fn host_access_requested(&self) -> bool {
        self.allow_filesystem
            || self.allow_network
            || self.allow_environment
            || self.allow_secret_access
    }
    #[must_use]
    pub const fn requires_review(&self) -> bool {
        self.host_access_requested()
            || matches!(
                self.kind,
                SandboxPolicyKind::None | SandboxPolicyKind::Unsupported
            )
    }
    #[must_use]
    pub const fn admission_status(&self) -> &'static str {
        if self.host_access_requested() {
            "review_required_host_access_requested"
        } else {
            match self.kind {
                SandboxPolicyKind::None => "review_required_no_sandbox_declared",
                SandboxPolicyKind::Unsupported => "unsupported_sandbox_policy",
                _ => "deny_by_default_no_host_access",
            }
        }
    }
    #[must_use]
    pub fn requested_host_access_summary(&self) -> String {
        let mut access = Vec::new();
        if self.allow_filesystem {
            access.push("filesystem");
        }
        if self.allow_network {
            access.push("network");
        }
        if self.allow_environment {
            access.push("environment");
        }
        if self.allow_secret_access {
            access.push("secret_access");
        }
        if access.is_empty() {
            "none".to_string()
        } else {
            access.join(",")
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "sandbox(kind={},safe_default={})",
            self.kind.as_str(),
            self.is_safe_default()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionPermission {
    pub permission: PermissionKind,
    pub required: bool,
    pub reason: String,
}
impl ExtensionPermission {
    #[must_use]
    pub fn required(permission: PermissionKind, reason: impl Into<String>) -> Self {
        Self {
            permission,
            required: true,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn optional(permission: PermissionKind, reason: impl Into<String>) -> Self {
        Self {
            permission,
            required: false,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        self.permission.is_effectful()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "permission:{} required={}",
            self.permission.as_str(),
            self.required
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionEffectDeclaration {
    pub effect: ExternalEffectKind,
    pub level: EffectLevel,
    pub requires_approval: bool,
    pub dry_run_safe: bool,
    pub idempotency_required: bool,
}
impl ExtensionEffectDeclaration {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            effect: ExternalEffectKind::None,
            level: EffectLevel::PureDeterministic,
            requires_approval: false,
            dry_run_safe: true,
            idempotency_required: false,
        }
    }
    #[must_use]
    pub const fn new(effect: ExternalEffectKind, level: EffectLevel) -> Self {
        let write = effect.is_write_or_mutation();
        Self {
            effect,
            level,
            requires_approval: write,
            dry_run_safe: !write,
            idempotency_required: write,
        }
    }
    #[must_use]
    pub const fn requires_approval(mut self, value: bool) -> Self {
        self.requires_approval = value;
        self
    }
    #[must_use]
    pub const fn dry_run_safe(mut self, value: bool) -> Self {
        self.dry_run_safe = value;
        self
    }
    #[must_use]
    pub const fn idempotency_required(mut self, value: bool) -> Self {
        self.idempotency_required = value;
        self
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        self.effect.is_effectful() || self.level.is_effectful()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "effect:{} level={}",
            self.effect.as_str(),
            self.level.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionDeterminismContract {
    PureDeterministic,
    PureNondeterministic,
    ExternalEffectBound,
    Unknown,
    Unsupported,
}

impl ExtensionDeterminismContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PureDeterministic => "pure_deterministic",
            Self::PureNondeterministic => "pure_nondeterministic",
            Self::ExternalEffectBound => "external_effect_bound",
            Self::Unknown => "unknown",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unknown | Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionMaterializationContract {
    MetadataOnly,
    EncodedNative,
    LateMaterialized,
    MaterializationRequired,
    Unsupported,
}

impl ExtensionMaterializationContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedNative => "encoded_native",
            Self::LateMaterialized => "late_materialized",
            Self::MaterializationRequired => "materialization_required",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }

    #[must_use]
    pub const fn requires_materialization(&self) -> bool {
        matches!(self, Self::MaterializationRequired)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionNullBehaviorContract {
    NullPropagating,
    NullSkipping,
    NullAware,
    NullError,
    Unknown,
    Unsupported,
}

impl ExtensionNullBehaviorContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NullPropagating => "null_propagating",
            Self::NullSkipping => "null_skipping",
            Self::NullAware => "null_aware",
            Self::NullError => "null_error",
            Self::Unknown => "unknown",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unknown | Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionRetryContract {
    None,
    IdempotentRetry,
    AtMostOnce,
    ManualReplayRequired,
    Unsupported,
}

impl ExtensionRetryContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::IdempotentRetry => "idempotent_retry",
            Self::AtMostOnce => "at_most_once",
            Self::ManualReplayRequired => "manual_replay_required",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionIdempotencyContract {
    NotRequired,
    Required,
    KeyRequired,
    Unsupported,
}

impl ExtensionIdempotencyContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Required => "required",
            Self::KeyRequired => "key_required",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionAuditContract {
    ManifestOnly,
    ExecutionCertificateRequired,
    FullAuditRequired,
    Unsupported,
}

impl ExtensionAuditContract {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ManifestOnly => "manifest_only",
            Self::ExecutionCertificateRequired => "execution_certificate_required",
            Self::FullAuditRequired => "full_audit_required",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_production_declared(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionExecutionContract {
    pub determinism: ExtensionDeterminismContract,
    pub materialization: ExtensionMaterializationContract,
    pub null_behavior: ExtensionNullBehaviorContract,
    pub input_dtypes: Vec<String>,
    pub output_dtype: Option<String>,
    pub timeout_millis: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_cpu_millis: Option<u64>,
    pub retry: ExtensionRetryContract,
    pub idempotency: ExtensionIdempotencyContract,
    pub audit: ExtensionAuditContract,
}

impl ExtensionExecutionContract {
    #[must_use]
    pub fn undeclared() -> Self {
        Self {
            determinism: ExtensionDeterminismContract::Unknown,
            materialization: ExtensionMaterializationContract::MetadataOnly,
            null_behavior: ExtensionNullBehaviorContract::Unknown,
            input_dtypes: vec![],
            output_dtype: None,
            timeout_millis: None,
            max_memory_bytes: None,
            max_cpu_millis: None,
            retry: ExtensionRetryContract::Unsupported,
            idempotency: ExtensionIdempotencyContract::Unsupported,
            audit: ExtensionAuditContract::Unsupported,
        }
    }

    #[must_use]
    pub fn metadata_only() -> Self {
        Self {
            determinism: ExtensionDeterminismContract::PureDeterministic,
            materialization: ExtensionMaterializationContract::MetadataOnly,
            null_behavior: ExtensionNullBehaviorContract::NullAware,
            input_dtypes: vec!["metadata".to_string()],
            output_dtype: Some("metadata".to_string()),
            timeout_millis: Some(1_000),
            max_memory_bytes: Some(16 * 1024 * 1024),
            max_cpu_millis: Some(1_000),
            retry: ExtensionRetryContract::None,
            idempotency: ExtensionIdempotencyContract::NotRequired,
            audit: ExtensionAuditContract::ManifestOnly,
        }
    }

    pub fn with_dtypes(
        mut self,
        input_dtypes: Vec<String>,
        output_dtype: impl Into<String>,
    ) -> Result<Self> {
        if input_dtypes.iter().any(|dtype| dtype.trim().is_empty()) {
            return Err(ShardLoomError::InvalidOperation(
                "extension execution contract input_dtypes must not contain empty values"
                    .to_string(),
            ));
        }
        let output_dtype = output_dtype.into();
        validate_non_empty("extension execution contract output_dtype", &output_dtype)?;
        self.input_dtypes = input_dtypes;
        self.output_dtype = Some(output_dtype);
        Ok(self)
    }

    #[must_use]
    pub const fn with_timeout_millis(mut self, value: u64) -> Self {
        self.timeout_millis = Some(value);
        self
    }

    #[must_use]
    pub const fn with_max_memory_bytes(mut self, value: u64) -> Self {
        self.max_memory_bytes = Some(value);
        self
    }

    #[must_use]
    pub const fn with_max_cpu_millis(mut self, value: u64) -> Self {
        self.max_cpu_millis = Some(value);
        self
    }

    #[must_use]
    pub const fn with_retry(mut self, retry: ExtensionRetryContract) -> Self {
        self.retry = retry;
        self
    }

    #[must_use]
    pub const fn with_idempotency(mut self, idempotency: ExtensionIdempotencyContract) -> Self {
        self.idempotency = idempotency;
        self
    }

    #[must_use]
    pub const fn with_audit(mut self, audit: ExtensionAuditContract) -> Self {
        self.audit = audit;
        self
    }

    #[must_use]
    pub const fn with_determinism(mut self, determinism: ExtensionDeterminismContract) -> Self {
        self.determinism = determinism;
        self
    }

    #[must_use]
    pub const fn with_materialization(
        mut self,
        materialization: ExtensionMaterializationContract,
    ) -> Self {
        self.materialization = materialization;
        self
    }

    #[must_use]
    pub const fn with_null_behavior(
        mut self,
        null_behavior: ExtensionNullBehaviorContract,
    ) -> Self {
        self.null_behavior = null_behavior;
        self
    }

    #[must_use]
    pub fn dtype_contract_declared(&self) -> bool {
        !self.input_dtypes.is_empty()
            && self
                .output_dtype
                .as_ref()
                .is_some_and(|dtype| !dtype.trim().is_empty())
    }

    #[must_use]
    pub fn resource_contract_declared(&self) -> bool {
        self.timeout_millis.is_some()
            && self.max_memory_bytes.is_some()
            && self.max_cpu_millis.is_some()
    }

    #[must_use]
    pub fn production_contract_complete(&self) -> bool {
        self.determinism.is_production_declared()
            && self.materialization.is_production_declared()
            && self.null_behavior.is_production_declared()
            && self.dtype_contract_declared()
            && self.resource_contract_declared()
            && self.retry.is_production_declared()
            && self.idempotency.is_production_declared()
            && self.audit.is_production_declared()
    }

    #[must_use]
    pub fn input_dtype_summary(&self) -> String {
        if self.input_dtypes.is_empty() {
            return "not_declared".to_string();
        }
        self.input_dtypes.join(",")
    }

    #[must_use]
    pub fn output_dtype_summary(&self) -> &str {
        self.output_dtype.as_deref().unwrap_or("not_declared")
    }

    #[must_use]
    pub fn resource_summary(&self) -> String {
        format!(
            "timeout_ms={},memory_bytes={},cpu_ms={}",
            self.timeout_millis
                .map_or_else(|| "not_declared".to_string(), |value| value.to_string()),
            self.max_memory_bytes
                .map_or_else(|| "not_declared".to_string(), |value| value.to_string()),
            self.max_cpu_millis
                .map_or_else(|| "not_declared".to_string(), |value| value.to_string())
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionLicenseKind {
    Apache2,
    Mit,
    Bsd,
    Isc,
    Zlib,
    Mpl2,
    Unknown,
    Incompatible,
}
impl ExtensionLicenseKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Apache2 => "Apache-2.0",
            Self::Mit => "MIT",
            Self::Bsd => "BSD",
            Self::Isc => "ISC",
            Self::Zlib => "Zlib",
            Self::Mpl2 => "MPL-2.0",
            Self::Unknown => "Unknown",
            Self::Incompatible => "Incompatible",
        }
    }
    #[must_use]
    pub const fn is_apache_compatible_candidate(&self) -> bool {
        matches!(
            self,
            Self::Apache2 | Self::Mit | Self::Bsd | Self::Isc | Self::Zlib
        )
    }
    #[must_use]
    pub const fn requires_review(&self) -> bool {
        matches!(self, Self::Mpl2 | Self::Unknown | Self::Incompatible)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionProvenance {
    pub source: Option<String>,
    pub homepage: Option<String>,
    pub license: ExtensionLicenseKind,
    pub notes: Option<String>,
}
impl ExtensionProvenance {
    #[must_use]
    pub const fn new(license: ExtensionLicenseKind) -> Self {
        Self {
            source: None,
            homepage: None,
            license,
            notes: None,
        }
    }
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
    #[must_use]
    pub fn with_homepage(mut self, homepage: impl Into<String>) -> Self {
        self.homepage = Some(homepage.into());
        self
    }
    #[must_use]
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    #[must_use]
    pub const fn requires_review(&self) -> bool {
        self.license.requires_review()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "license={},review={}",
            self.license.as_str(),
            self.requires_review()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionManifest {
    pub id: ExtensionId,
    pub name: String,
    pub version: ExtensionVersion,
    pub provider: Option<String>,
    pub category: ExtensionCategory,
    pub lifecycle: ExtensionLifecycleState,
    pub capabilities: Vec<ExtensionCapability>,
    pub permissions: Vec<ExtensionPermission>,
    pub effects: Vec<ExtensionEffectDeclaration>,
    pub sandbox: SandboxPolicy,
    pub abi: Option<PluginAbiRequirement>,
    pub runtime: Option<UdfRuntimeKind>,
    pub execution_contract: ExtensionExecutionContract,
    pub provenance: ExtensionProvenance,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExtensionManifest {
    pub fn new(
        id: ExtensionId,
        name: impl Into<String>,
        version: ExtensionVersion,
        category: ExtensionCategory,
        provenance: ExtensionProvenance,
    ) -> Result<Self> {
        let n = name.into();
        validate_non_empty("extension name", &n)?;
        Ok(Self {
            id,
            name: n,
            version,
            provider: None,
            category,
            lifecycle: ExtensionLifecycleState::Discovered,
            capabilities: vec![],
            permissions: vec![],
            effects: vec![],
            sandbox: SandboxPolicy::metadata_only(),
            abi: None,
            runtime: None,
            execution_contract: ExtensionExecutionContract::undeclared(),
            provenance,
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }
    #[must_use]
    pub const fn with_lifecycle(mut self, lifecycle: ExtensionLifecycleState) -> Self {
        self.lifecycle = lifecycle;
        self
    }
    #[must_use]
    pub fn with_abi(mut self, abi: PluginAbiRequirement) -> Self {
        self.abi = Some(abi);
        self
    }
    #[must_use]
    pub const fn with_runtime(mut self, runtime: UdfRuntimeKind) -> Self {
        self.runtime = Some(runtime);
        self
    }
    #[must_use]
    pub fn with_execution_contract(mut self, contract: ExtensionExecutionContract) -> Self {
        self.execution_contract = contract;
        self
    }
    #[must_use]
    pub fn with_sandbox(mut self, sandbox: SandboxPolicy) -> Self {
        self.sandbox = sandbox;
        self
    }
    pub fn add_capability(&mut self, capability: ExtensionCapability) {
        self.capabilities.push(capability)
    }
    pub fn add_permission(&mut self, permission: ExtensionPermission) {
        self.permissions.push(permission)
    }
    pub fn add_effect(&mut self, effect: ExtensionEffectDeclaration) {
        self.effects.push(effect)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic)
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.lifecycle.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.lifecycle.is_usable()
            && !self.has_errors()
            && !self.requires_review()
            && !self.diagnostics.iter().any(|d| {
                d.code == DiagnosticCode::NoFallbackExecution
                    || matches!(
                        d.code,
                        DiagnosticCode::NotImplemented
                            | DiagnosticCode::UnsupportedEffect
                            | DiagnosticCode::UnsupportedUdf
                    )
            })
    }
    #[must_use]
    pub fn requires_review(&self) -> bool {
        self.provenance.requires_review()
            || self.sandbox.requires_review()
            || self.runtime_requires_review()
            || !self.execution_contract.production_contract_complete()
            || self.capabilities.iter().any(ExtensionCapability::is_usable)
            || self
                .permissions
                .iter()
                .any(ExtensionPermission::is_effectful)
            || self
                .effects
                .iter()
                .any(ExtensionEffectDeclaration::is_effectful)
    }
    #[must_use]
    pub const fn runtime_requires_review(&self) -> bool {
        match self.runtime {
            Some(runtime) => !runtime.is_available_initially(),
            None => false,
        }
    }
    #[must_use]
    pub const fn runtime_admission_status(&self) -> &'static str {
        match self.runtime {
            None => "not_declared_metadata_only",
            Some(UdfRuntimeKind::BuiltinDeterministicFixture) => "builtin_fixture_only",
            Some(UdfRuntimeKind::RustNative) => {
                "blocked_rust_native_requires_abi_sandbox_certificate"
            }
            Some(UdfRuntimeKind::Wasm) => {
                "blocked_wasm_requires_runtime_fuel_memory_timeout_sandbox"
            }
            Some(UdfRuntimeKind::Python) => {
                "blocked_python_requires_materialization_sandbox_policy"
            }
            Some(UdfRuntimeKind::SqlDefined) => {
                "blocked_sql_defined_requires_planner_function_registry"
            }
            Some(UdfRuntimeKind::ExternalService) => "blocked_external_service_denied_by_default",
            Some(UdfRuntimeKind::Unknown) => "blocked_unknown_runtime",
        }
    }
    #[must_use]
    pub fn effect_execution_admission_status(&self) -> &'static str {
        if self
            .permissions
            .iter()
            .any(ExtensionPermission::is_effectful)
            || self
                .effects
                .iter()
                .any(ExtensionEffectDeclaration::is_effectful)
        {
            "denied_by_default_external_effect"
        } else if self.sandbox.host_access_requested() {
            "denied_by_default_host_access"
        } else if self.runtime_requires_review() {
            "denied_by_default_runtime"
        } else {
            "metadata_only_non_executing"
        }
    }
    #[must_use]
    pub fn review_reason_codes(&self) -> Vec<&'static str> {
        let mut reasons = Vec::new();
        if self.provenance.requires_review() {
            reasons.push("license_or_provenance_review_required");
        }
        if self
            .permissions
            .iter()
            .any(ExtensionPermission::is_effectful)
        {
            reasons.push("effectful_permission_declared");
        }
        if self
            .effects
            .iter()
            .any(ExtensionEffectDeclaration::is_effectful)
        {
            reasons.push("effectful_operation_declared");
        }
        if self.capabilities.iter().any(ExtensionCapability::is_usable) {
            reasons.push("supported_capability_claim_declared");
        }
        if self.sandbox.host_access_requested() {
            reasons.push("sandbox_host_access_requested");
        }
        if matches!(self.sandbox.kind, SandboxPolicyKind::None) {
            reasons.push("sandbox_policy_none_declared");
        }
        if matches!(self.sandbox.kind, SandboxPolicyKind::Unsupported) {
            reasons.push("unsupported_sandbox_policy_declared");
        }
        if self.runtime_requires_review() {
            reasons.push("runtime_requires_sandbox_or_bridge_review");
        }
        if !self.execution_contract.production_contract_complete() {
            reasons.push("execution_contract_incomplete");
        }
        reasons
    }
    #[must_use]
    pub fn has_effects(&self) -> bool {
        self.effects
            .iter()
            .any(ExtensionEffectDeclaration::is_effectful)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "extension {} {} category={} lifecycle={} metadata_only=true",
            self.id.as_str(),
            self.version.summary(),
            self.category.as_str(),
            self.lifecycle.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionInspectionStatus {
    MetadataOnly,
    Validated,
    RequiresReview,
    Unsafe,
    Unsupported,
    NotImplemented,
}
impl ExtensionInspectionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Validated => "validated",
            Self::RequiresReview => "requires_review",
            Self::Unsafe => "unsafe",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::Unsafe | Self::Unsupported | Self::NotImplemented
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionInspectionReport {
    pub manifest: ExtensionManifest,
    pub status: ExtensionInspectionStatus,
    pub code_executed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExtensionInspectionReport {
    #[must_use]
    pub fn metadata_only(manifest: ExtensionManifest) -> Self {
        Self {
            manifest,
            status: ExtensionInspectionStatus::MetadataOnly,
            code_executed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn requires_review(manifest: ExtensionManifest, reason: impl Into<String>) -> Self {
        let mut out = Self::metadata_only(manifest);
        out.status = ExtensionInspectionStatus::RequiresReview;
        out.diagnostics.push(Diagnostic::new(
            DiagnosticCode::ConfigurationError,
            DiagnosticSeverity::Warning,
            crate::DiagnosticCategory::Configuration,
            "Extension requires manual review.",
            Some("extension_inspection".to_string()),
            Some(reason.into()),
            Some("Review extension provenance and permissions.".to_string()),
            crate::FallbackStatus::disabled_by_policy(),
        ));
        out
    }
    #[must_use]
    pub fn unsupported(
        manifest: ExtensionManifest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self::metadata_only(manifest);
        out.status = ExtensionInspectionStatus::Unsupported;
        out.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature.into(),
            reason.into(),
            Some("Fallback execution remains disabled.".to_string()),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic)
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
    pub fn to_human_text(&self) -> String {
        format!(
            "extension_inspection status={} code_executed={} fallback_execution=disabled\n{}",
            self.status.as_str(),
            self.code_executed,
            self.manifest.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionRegistrySnapshot {
    pub manifests: Vec<ExtensionManifest>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExtensionRegistrySnapshot {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            manifests: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_manifest(&mut self, manifest: ExtensionManifest) {
        self.manifests.push(manifest)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic)
    }
    #[must_use]
    pub fn extension_count(&self) -> usize {
        self.manifests.len()
    }
    #[must_use]
    pub fn usable_count(&self) -> usize {
        self.manifests.iter().filter(|m| m.is_usable()).count()
    }
    #[must_use]
    pub fn requires_review_count(&self) -> usize {
        self.manifests
            .iter()
            .filter(|m| m.requires_review())
            .count()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self.manifests.iter().any(ExtensionManifest::has_errors)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "extension_registry count={} usable={} review_required={} fallback_execution=disabled extension_code_executed=false",
            self.extension_count(),
            self.usable_count(),
            self.requires_review_count()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionManifestEffectCapabilityRow {
    pub row_id: &'static str,
    pub extension_type: &'static str,
    pub support_status: &'static str,
    pub manifest_status: &'static str,
    pub required_permissions: &'static str,
    pub sandbox_policy: &'static str,
    pub effect_metadata: &'static str,
    pub materialization_boundary_required: bool,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub runtime_execution: bool,
    pub extension_code_executed: bool,
    pub dynamic_loading: bool,
    pub udf_execution: bool,
    pub external_effect_executed: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dependency_expansion_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl ExtensionManifestEffectCapabilityRow {
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    const fn new(
        row_id: &'static str,
        extension_type: &'static str,
        support_status: &'static str,
        manifest_status: &'static str,
        required_permissions: &'static str,
        sandbox_policy: &'static str,
        effect_metadata: &'static str,
        materialization_boundary_required: bool,
        blocker_id: &'static str,
        diagnostic_code: &'static str,
        required_evidence: &'static str,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            extension_type,
            support_status,
            manifest_status,
            required_permissions,
            sandbox_policy,
            effect_metadata,
            materialization_boundary_required,
            blocker_id,
            diagnostic_code,
            required_evidence,
            runtime_execution: false,
            extension_code_executed: false,
            dynamic_loading: false,
            udf_execution: false,
            external_effect_executed: false,
            credential_resolution_performed: false,
            network_probe_performed: false,
            dependency_expansion_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionManifestEffectCapabilityMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub docs_ref: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<ExtensionManifestEffectCapabilityRow>,
    pub runtime_execution: bool,
    pub extension_code_executed: bool,
    pub dynamic_loading: bool,
    pub udf_execution: bool,
    pub external_effect_executed: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dependency_expansion_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl ExtensionManifestEffectCapabilityMatrix {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.extension_manifest_effect_capability_matrix.v1",
            matrix_id: "gar-0011-a.extension_manifest_external_effect_capability_matrix",
            docs_ref: "docs/architecture/extension-manifest-effect-capability-matrix.md",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                ExtensionManifestEffectCapabilityRow::new(
                    "metadata_only_manifest",
                    "manifest",
                    "report_only",
                    "metadata_only_validated_no_load",
                    "none",
                    "metadata_only",
                    "none",
                    false,
                    "none_metadata_only_manifest",
                    "SL_EXTENSION_METADATA_ONLY",
                    "manifest_schema,provenance_fields,no_fallback_evidence",
                    "Metadata-only extension manifest inspection may be reported; it does not load code or enable runtime support.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "sql_frontend_extension",
                    "frontend",
                    "report_only",
                    "manifest_only",
                    "plan_metadata_only",
                    "metadata_only",
                    "none",
                    true,
                    "gar-0011-a.sql_frontend_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "parser_contract,binder_contract,planner_contract,semantic_tests,effect_budget_certificate,no_fallback_evidence",
                    "SQL frontend extensions remain report-only; no parser, binder, planner, runtime, or fallback execution is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "rust_udf_extension",
                    "scalar_udf",
                    "blocked",
                    "manifest_only",
                    "execute_udf,read_local,write_local_optional",
                    "full_sandbox_required",
                    "determinism_and_materialization_declared",
                    true,
                    "gar-0011-a.rust_udf_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "abi_contract,function_registry,sandbox_policy,determinism_policy,execution_certificate,no_fallback_evidence",
                    "Rust UDF extensions remain blocked until ABI, registry, sandbox, determinism, and evidence contracts admit execution.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "wasm_udf_extension",
                    "wasm_udf",
                    "blocked",
                    "manifest_only",
                    "execute_udf",
                    "full_sandbox_required",
                    "fuel_memory_and_effects_declared",
                    true,
                    "gar-0011-a.wasm_udf_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "wasm_runtime_policy,fuel_budget,memory_budget,sandbox_policy,execution_certificate,no_fallback_evidence",
                    "WASM UDF extensions remain blocked; no WASM runtime, fuel budget, memory budget, or execution claim is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "python_udf_extension",
                    "python_udf",
                    "blocked",
                    "manifest_only",
                    "execute_udf,materialize_rows",
                    "full_sandbox_required",
                    "python_boundary_and_materialization_declared",
                    true,
                    "gar-0011-a.python_udf_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "python_boundary,materialization_policy,sandbox_policy,redaction_policy,execution_certificate,no_fallback_evidence",
                    "Python UDF extensions remain blocked; no Python function execution, row materialization, data egress, or fallback path is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "encoded_kernel_extension",
                    "encoded_kernel",
                    "blocked",
                    "manifest_only",
                    "execute_kernel,read_encoded_segments",
                    "full_sandbox_required",
                    "encoding_and_decode_boundary_declared",
                    true,
                    "gar-0011-a.encoded_kernel_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "kernel_registry,encoding_support_matrix,correctness_tests,decode_materialization_evidence,no_fallback_evidence",
                    "Encoded-kernel extensions remain blocked until kernel registry, encoding support, correctness, and decode-boundary evidence exist.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "translation_sink_extension",
                    "translation_sink",
                    "blocked",
                    "manifest_only",
                    "write_output",
                    "full_sandbox_required",
                    "sink_io_and_replay_declared",
                    true,
                    "gar-0011-a.translation_sink_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "sink_contract,result_replay_evidence,output_native_io_certificate,commit_policy,no_fallback_evidence",
                    "Translation sink extensions remain blocked; no sink write, commit, replay, or production output claim is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "connector_extension",
                    "connector",
                    "blocked",
                    "manifest_only",
                    "read_source,write_sink_optional",
                    "full_sandbox_required",
                    "source_sink_effects_declared",
                    true,
                    "gar-0011-a.connector_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "adapter_contract,credential_policy,network_policy,source_sink_certificates,no_fallback_evidence",
                    "Connector extensions remain blocked; no database, SaaS, object-store, or external adapter runtime is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "object_store_provider_extension",
                    "object_store_provider",
                    "blocked",
                    "manifest_only",
                    "object_store_read,object_store_write_optional",
                    "full_sandbox_required",
                    "credential_network_commit_effects_declared",
                    true,
                    "gar-0011-a.object_store_provider_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "credential_policy,network_policy,byte_range_evidence,commit_protocol,Native_IO_certificate,no_fallback_evidence",
                    "Object-store provider extensions remain blocked; no credential resolution, network probe, read, write, or commit runtime is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "catalog_provider_extension",
                    "catalog_provider",
                    "blocked",
                    "manifest_only",
                    "catalog_read,catalog_write_optional",
                    "full_sandbox_required",
                    "catalog_effects_declared",
                    true,
                    "gar-0011-a.catalog_provider_extension_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "catalog_contract,credential_policy,table_metadata_policy,transaction_policy,no_fallback_evidence",
                    "Catalog provider extensions remain blocked; no catalog probe, table metadata runtime, transaction, or fallback path is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "api_llm_effect_provider",
                    "effect_provider",
                    "blocked",
                    "manifest_only",
                    "call_api,call_model,network_egress",
                    "full_sandbox_required",
                    "external_call_cost_redaction_and_audit_declared",
                    true,
                    "gar-0011-a.api_llm_effect_provider_runtime_blocked",
                    "SL_BLOCKED_EXTERNAL_EFFECT",
                    "credential_policy,network_policy,model_policy,cost_budget,redaction_policy,audit_trail,no_fallback_evidence",
                    "API and LLM effect providers remain denied by default; no credential use, network call, prompt/data egress, or model invocation is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "embedding_vector_provider",
                    "embedding_vector_provider",
                    "blocked",
                    "manifest_only",
                    "call_model,vector_index_read,network_egress",
                    "full_sandbox_required",
                    "embedding_vector_effects_declared",
                    true,
                    "gar-0011-a.embedding_vector_provider_runtime_blocked",
                    "SL_BLOCKED_EXTERNAL_EFFECT",
                    "model_policy,credential_policy,network_policy,vector_schema,redaction_policy,no_fallback_evidence",
                    "Embedding and vector providers remain blocked; no model call, vector generation, remote index query, or vector-runtime claim is enabled.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "observability_exporter_extension",
                    "observability_exporter",
                    "report_only",
                    "manifest_only",
                    "export_evidence_optional",
                    "metadata_only",
                    "export_disabled_by_default",
                    false,
                    "gar-0011-a.observability_exporter_runtime_blocked",
                    "SL_BLOCKED_EXTERNAL_EFFECT",
                    "export_schema,redaction_policy,network_policy,opt_in_policy,no_fallback_evidence",
                    "Observability exporter extensions remain report-only and opt-in; no lineage or telemetry event is emitted by default.",
                ),
                ExtensionManifestEffectCapabilityRow::new(
                    "benchmark_provider_extension",
                    "benchmark_provider",
                    "report_only",
                    "manifest_only",
                    "external_baseline_optional",
                    "metadata_only",
                    "external_baseline_only",
                    false,
                    "gar-0011-a.benchmark_provider_runtime_blocked",
                    "SL_BLOCKED_EXTENSION_RUNTIME",
                    "benchmark_profile,baseline_boundary,dependency_policy,claim_gate,no_fallback_evidence",
                    "Benchmark provider extensions are external-baseline-only; they cannot satisfy ShardLoom runtime or fallback execution.",
                ),
            ],
            runtime_execution: false,
            extension_code_executed: false,
            dynamic_loading: false,
            udf_execution: false,
            external_effect_executed: false,
            credential_resolution_performed: false,
            network_probe_performed: false,
            dependency_expansion_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
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
    pub fn all_runtime_blocked(&self) -> bool {
        !self.runtime_execution
            && !self.extension_code_executed
            && !self.dynamic_loading
            && !self.udf_execution
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self.rows.iter().all(|row| {
                !row.runtime_execution
                    && !row.extension_code_executed
                    && !row.dynamic_loading
                    && !row.udf_execution
                    && !row.fallback_attempted
                    && !row.external_engine_invoked
            })
    }

    #[must_use]
    pub fn all_external_effects_blocked(&self) -> bool {
        !self.external_effect_executed
            && !self.credential_resolution_performed
            && !self.network_probe_performed
            && !self.dependency_expansion_allowed
            && self.rows.iter().all(|row| {
                !row.external_effect_executed
                    && !row.credential_resolution_performed
                    && !row.network_probe_performed
                    && !row.dependency_expansion_allowed
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAbiUdfSandboxBlockerRow {
    pub row_id: &'static str,
    pub plugin_surface: &'static str,
    pub support_status: &'static str,
    pub abi_status: &'static str,
    pub sandbox_requirement: &'static str,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub user_visible_surface: &'static str,
    pub dynamic_loading_performed: bool,
    pub extension_code_executed: bool,
    pub udf_execution_performed: bool,
    pub sandbox_enforced: bool,
    pub permission_policy_enforced: bool,
    pub runtime_execution: bool,
    pub external_effect_executed: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dependency_expansion_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl PluginAbiUdfSandboxBlockerRow {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        row_id: &'static str,
        plugin_surface: &'static str,
        support_status: &'static str,
        abi_status: &'static str,
        sandbox_requirement: &'static str,
        blocker_id: &'static str,
        diagnostic_code: &'static str,
        required_evidence: &'static str,
        user_visible_surface: &'static str,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            plugin_surface,
            support_status,
            abi_status,
            sandbox_requirement,
            blocker_id,
            diagnostic_code,
            required_evidence,
            user_visible_surface,
            dynamic_loading_performed: false,
            extension_code_executed: false,
            udf_execution_performed: false,
            sandbox_enforced: false,
            permission_policy_enforced: false,
            runtime_execution: false,
            external_effect_executed: false,
            credential_resolution_performed: false,
            network_probe_performed: false,
            dependency_expansion_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAbiUdfSandboxBlockerReport {
    pub schema_version: &'static str,
    pub blocker_id: &'static str,
    pub docs_ref: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<PluginAbiUdfSandboxBlockerRow>,
    pub abi_loading_supported: bool,
    pub dynamic_loading_performed: bool,
    pub extension_code_executed: bool,
    pub udf_execution_performed: bool,
    pub sandbox_evidence_required: bool,
    pub sandbox_enforced: bool,
    pub permission_policy_enforced: bool,
    pub runtime_execution: bool,
    pub external_effect_executed: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dependency_expansion_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl PluginAbiUdfSandboxBlockerReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.plugin_abi_udf_sandbox_blocker.v1",
            blocker_id: "gar-0023-a.plugin_abi_udf_sandbox_blocker",
            docs_ref: "docs/architecture/plugin-abi-udf-sandbox-blocker.md",
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                PluginAbiUdfSandboxBlockerRow::new(
                    "abi_contract_inventory",
                    "plugin_abi_contract",
                    "report_only",
                    "metadata_only_not_stable",
                    "sandbox_evidence_required",
                    "none_abi_inventory",
                    "SL_PLUGIN_ABI_REPORT_ONLY",
                    "abi_schema,version_policy,manifest_schema,sandbox_policy,no_fallback_evidence",
                    "extension-registry,extension-inspect,capabilities extensions",
                    "Plugin ABI metadata can be inventoried, but ShardLoom does not stabilize or load plugin ABI runtime code.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "dynamic_library_loading",
                    "dynamic_loading",
                    "blocked",
                    "unsupported",
                    "full_sandbox_required",
                    "gar-0023-a.dynamic_loading_blocked",
                    "SL_PLUGIN_ABI_BLOCKED",
                    "abi_compatibility,signature_verification,dependency_isolation,sandbox_policy,provenance,no_fallback_evidence",
                    "extension-inspect,capabilities extensions",
                    "Dynamic library loading remains blocked; extension inspection cannot execute code or load native libraries.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "rust_native_udf",
                    "rust_udf",
                    "blocked",
                    "unsupported",
                    "full_sandbox_required",
                    "gar-0023-a.rust_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "function_registry,abi_contract,type_contract,determinism_policy,sandbox_policy,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs",
                    "Rust-native UDF execution remains blocked until ABI, function registry, sandbox, and certificate evidence exist.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "wasm_udf",
                    "wasm_udf",
                    "blocked",
                    "unsupported",
                    "fuel_memory_timeout_sandbox_required",
                    "gar-0023-a.wasm_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "wasm_runtime_policy,fuel_budget,memory_budget,timeout_policy,sandbox_policy,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs",
                    "WASM UDF execution remains blocked; no WASM runtime, fuel budget, memory enforcement, or timeout enforcement is claimed.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "python_udf",
                    "python_udf",
                    "blocked",
                    "unsupported",
                    "python_boundary_and_sandbox_required",
                    "gar-0023-a.python_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "python_boundary,materialization_policy,redaction_policy,sandbox_policy,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs",
                    "Python UDF execution remains blocked; no Python callable, interpreter bridge, row materialization, or fallback execution is enabled.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "sql_defined_udf",
                    "sql_defined_udf",
                    "blocked",
                    "unsupported",
                    "planner_and_function_registry_required",
                    "gar-0023-a.sql_defined_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "sql_parser,binder,planner,function_registry,semantic_tests,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs",
                    "SQL-defined UDF execution remains blocked until SQL frontend and function registry evidence exists.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "external_service_udf",
                    "external_service_udf",
                    "blocked",
                    "unsupported",
                    "network_credentials_and_effect_policy_required",
                    "gar-0023-a.external_service_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "network_policy,credential_policy,effect_budget,redaction_policy,audit_trail,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs,capabilities security-governance",
                    "External-service UDF execution remains blocked; no network, credential resolution, API call, or external effect is performed.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "table_function_udf",
                    "table_function_udf",
                    "blocked",
                    "unsupported",
                    "source_sink_materialization_policy_required",
                    "gar-0023-a.table_function_udf_blocked",
                    "SL_UDF_SANDBOX_BLOCKED",
                    "table_function_contract,source_sink_policy,materialization_boundary,execution_certificate,no_fallback_evidence",
                    "udf-runtime-plan,capabilities udfs",
                    "Table-function UDF execution remains blocked until source/sink and materialization boundaries are certified.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "plugin_lifecycle_transition",
                    "plugin_lifecycle",
                    "blocked",
                    "unsupported",
                    "manifest_validation_and_policy_required",
                    "gar-0023-a.plugin_lifecycle_transition_blocked",
                    "SL_PLUGIN_ABI_BLOCKED",
                    "manifest_validation,abi_status,provenance,signature,sandbox_policy,audit_trail,no_fallback_evidence",
                    "extension-registry,extension-inspect",
                    "Plugin lifecycle transitions beyond metadata inspection remain blocked; discovered metadata is not loaded, enabled, or executed.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "sandbox_evidence_binding",
                    "sandbox_evidence",
                    "blocked",
                    "unsupported",
                    "gar-0019-a_and_gar-0019-b_required",
                    "gar-0023-a.sandbox_evidence_binding_blocked",
                    "SL_PLUGIN_ABI_BLOCKED",
                    "credential_policy_gate,sandbox_governance_gate,effect_budget,execution_certificate,no_fallback_evidence",
                    "capabilities security-governance,capabilities extensions,capabilities udfs",
                    "Plugin/UDF runtime admission remains blocked until credential and sandbox governance gates are evidence-backed runtime gates.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "license_provenance_attestation",
                    "license_provenance",
                    "report_only",
                    "metadata_only",
                    "review_required_before_enablement",
                    "gar-0023-a.license_provenance_runtime_blocked",
                    "SL_PLUGIN_ABI_REPORT_ONLY",
                    "license_kind,source_ref,dependency_manifest,notice_policy,supply_chain_attestation,no_fallback_evidence",
                    "extension-inspect,release security gate",
                    "License and provenance metadata may be inspected; it does not authorize dependency expansion, plugin loading, or runtime support.",
                ),
                PluginAbiUdfSandboxBlockerRow::new(
                    "unsupported_diagnostics",
                    "diagnostics",
                    "report_only",
                    "metadata_only",
                    "deterministic_unsupported_without_execution",
                    "none_diagnostic_only",
                    "SL_PLUGIN_ABI_UNSUPPORTED",
                    "diagnostic_code,blocker_id,claim_boundary,no_fallback_evidence",
                    "extension-registry,extension-inspect,udf-runtime-plan,capabilities extensions,capabilities udfs",
                    "Unsupported plugin ABI and UDF sandbox requests must emit deterministic diagnostics without loading code, executing UDFs, or invoking fallback engines.",
                ),
            ],
            abi_loading_supported: false,
            dynamic_loading_performed: false,
            extension_code_executed: false,
            udf_execution_performed: false,
            sandbox_evidence_required: true,
            sandbox_enforced: false,
            permission_policy_enforced: false,
            runtime_execution: false,
            external_effect_executed: false,
            credential_resolution_performed: false,
            network_probe_performed: false,
            dependency_expansion_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
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
    pub fn all_plugin_runtime_blocked(&self) -> bool {
        !self.abi_loading_supported
            && !self.dynamic_loading_performed
            && !self.extension_code_executed
            && !self.udf_execution_performed
            && self.sandbox_evidence_required
            && !self.sandbox_enforced
            && !self.permission_policy_enforced
            && !self.runtime_execution
            && !self.external_effect_executed
            && !self.credential_resolution_performed
            && !self.network_probe_performed
            && !self.dependency_expansion_allowed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self.rows.iter().all(|row| {
                !row.dynamic_loading_performed
                    && !row.extension_code_executed
                    && !row.udf_execution_performed
                    && !row.sandbox_enforced
                    && !row.permission_policy_enforced
                    && !row.runtime_execution
                    && !row.external_effect_executed
                    && !row.credential_resolution_performed
                    && !row.network_probe_performed
                    && !row.dependency_expansion_allowed
                    && !row.fallback_attempted
                    && !row.external_engine_invoked
            })
    }
}

#[must_use]
pub fn plan_plugin_abi_udf_sandbox_blocker() -> PluginAbiUdfSandboxBlockerReport {
    PluginAbiUdfSandboxBlockerReport::report_only()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extension_id_rejects_empty() {
        assert!(ExtensionId::new(" ").is_err())
    }
    #[test]
    fn extension_version_summary_works() {
        assert_eq!(ExtensionVersion::new(1, 2, 3).summary(), "1.2.3");
        assert_eq!(
            ExtensionVersion::new(1, 2, 3)
                .with_pre_release("rc1")
                .expect("ok")
                .summary(),
            "1.2.3-rc1"
        )
    }
    #[test]
    fn extension_version_rejects_empty_pre() {
        assert!(
            ExtensionVersion::new(1, 0, 0)
                .with_pre_release(" ")
                .is_err()
        )
    }
    #[test]
    fn llm_is_effect_provider() {
        assert!(ExtensionCategory::LlmProvider.is_effect_provider())
    }
    #[test]
    fn encoded_kernel_execution_related() {
        assert!(ExtensionCategory::EncodedKernel.is_execution_related())
    }
    #[test]
    fn lifecycle_enabled_usable() {
        assert!(ExtensionLifecycleState::Enabled.is_usable())
    }
    #[test]
    fn lifecycle_failed_error() {
        assert!(ExtensionLifecycleState::Failed.is_error())
    }
    #[test]
    fn capability_rejects_empty() {
        assert!(ExtensionCapability::new("", ExtensionCapabilityStatus::Supported).is_err())
    }
    #[test]
    fn planned_not_usable() {
        assert!(!ExtensionCapabilityStatus::Planned.is_usable())
    }
    #[test]
    fn abi_rejects_empty_api() {
        assert!(PluginAbiRequirement::new("", ExtensionVersion::new(1, 0, 0)).is_err())
    }
    #[test]
    fn abi_compatible_loads() {
        assert!(PluginAbiStatus::Compatible.allows_loading())
    }
    #[test]
    fn abi_experimental_not_loads() {
        assert!(!PluginAbiStatus::Experimental.allows_loading())
    }
    #[test]
    fn python_requires_sandbox() {
        assert!(UdfRuntimeKind::Python.requires_sandboxing())
    }
    #[test]
    fn python_not_available() {
        assert!(!UdfRuntimeKind::Python.is_available_initially())
    }
    #[test]
    fn builtin_fixture_udf_available_without_sandbox() {
        assert!(UdfRuntimeKind::BuiltinDeterministicFixture.is_available_initially());
        assert!(!UdfRuntimeKind::BuiltinDeterministicFixture.requires_sandboxing());
    }
    #[test]
    fn typed_udf_registry_declares_scalar_aggregate_and_table_boundaries() {
        let report = typed_udf_registry_report();
        assert_eq!(report.schema_version, "shardloom.typed_udf_registry.v1");
        assert_eq!(report.claim_gate_status, "fixture_smoke_only");
        assert_eq!(report.admitted_local_fixture_count(), 1);
        assert_eq!(report.scalar_count(), 2);
        assert_eq!(report.aggregate_count(), 1);
        assert_eq!(report.table_function_count(), 1);
        assert_eq!(report.encoded_native_candidate_count(), 1);
        assert_eq!(report.materialization_required_count(), 2);
        assert!(report.local_fixture_execution_bridge_available);
        assert!(!report.arbitrary_runtime_bridge_available);
        assert!(report.sandbox_policy_declared);
        assert!(report.side_effect_free());
        for row_id in [
            "sl_fixture_double_i64",
            "sl_native_sum_i64",
            "sl_table_generate_series_i64",
            "external_python_scalar_boundary",
        ] {
            assert!(report.row_order().contains(&row_id), "missing {row_id}");
        }
    }
    #[test]
    fn typed_udf_registry_admits_only_builtin_scalar_fixture() {
        let report = typed_udf_registry_report();
        let admitted = report
            .entries
            .iter()
            .filter(|entry| entry.is_admitted())
            .collect::<Vec<_>>();
        assert_eq!(admitted.len(), 1);
        let fixture = admitted[0];
        assert_eq!(fixture.udf_id, "sl_fixture_double_i64");
        assert_eq!(fixture.kind, TypedUdfKind::Scalar);
        assert_eq!(
            fixture.runtime_kind,
            UdfRuntimeKind::BuiltinDeterministicFixture
        );
        assert_eq!(
            fixture.runtime_fixture_command,
            Some("udf-local-scalar-fixture-smoke")
        );
        assert_eq!(
            fixture.encoded_capability,
            TypedUdfEncodedCapability::LateMaterializedFixture
        );
        assert!(!fixture.materialization_required());
        assert!(fixture.no_fallback_invariant_holds());
        assert!(
            report
                .entries
                .iter()
                .filter(|entry| !entry.is_admitted())
                .all(|entry| !entry.registry_execution_allowed)
        );
    }
    #[test]
    fn metadata_policy_safe() {
        assert!(SandboxPolicy::metadata_only().is_safe_default())
    }
    #[test]
    fn metadata_execution_contract_is_complete() {
        let contract = ExtensionExecutionContract::metadata_only();
        assert!(contract.production_contract_complete());
        assert_eq!(contract.determinism.as_str(), "pure_deterministic");
        assert_eq!(contract.materialization.as_str(), "metadata_only");
        assert_eq!(contract.null_behavior.as_str(), "null_aware");
        assert_eq!(contract.input_dtype_summary(), "metadata");
        assert_eq!(contract.output_dtype_summary(), "metadata");
        assert!(contract.resource_contract_declared());
    }
    #[test]
    fn undeclared_execution_contract_requires_review() {
        let contract = ExtensionExecutionContract::undeclared();
        assert!(!contract.production_contract_complete());
        assert!(!contract.dtype_contract_declared());
        assert!(!contract.resource_contract_declared());
    }
    #[test]
    fn network_policy_not_safe() {
        assert!(
            !SandboxPolicy::metadata_only()
                .allow_network(true)
                .is_safe_default()
        )
    }
    #[test]
    fn host_access_policy_requires_review() {
        let policy = SandboxPolicy::metadata_only()
            .allow_filesystem(true)
            .allow_network(true)
            .allow_secret_access(true);
        assert!(policy.host_access_requested());
        assert!(policy.requires_review());
        assert_eq!(
            policy.admission_status(),
            "review_required_host_access_requested"
        );
        assert_eq!(
            policy.requested_host_access_summary(),
            "filesystem,network,secret_access"
        );
    }
    #[test]
    fn sandbox_none_and_unsupported_require_review() {
        let none = SandboxPolicy {
            kind: SandboxPolicyKind::None,
            allow_filesystem: false,
            allow_network: false,
            allow_environment: false,
            allow_secret_access: false,
            max_memory_bytes: None,
            max_runtime_millis: None,
        };
        assert!(none.requires_review());
        assert_eq!(
            none.admission_status(),
            "review_required_no_sandbox_declared"
        );

        let unsupported = SandboxPolicy {
            kind: SandboxPolicyKind::Unsupported,
            ..none
        };
        assert!(unsupported.requires_review());
        assert_eq!(unsupported.admission_status(), "unsupported_sandbox_policy");
    }
    #[test]
    fn call_api_permission_effectful() {
        assert!(ExtensionPermission::required(PermissionKind::CallApi, "x").is_effectful())
    }
    #[test]
    fn none_effect_not_effectful() {
        assert!(!ExtensionEffectDeclaration::none().is_effectful())
    }
    #[test]
    fn apache_candidate() {
        assert!(ExtensionLicenseKind::Apache2.is_apache_compatible_candidate())
    }
    #[test]
    fn mpl_requires_review() {
        assert!(ExtensionLicenseKind::Mpl2.requires_review())
    }
    #[test]
    fn unknown_provenance_review() {
        assert!(ExtensionProvenance::new(ExtensionLicenseKind::Unknown).requires_review())
    }
    #[test]
    fn manifest_rejects_empty_name() {
        let id = ExtensionId::new("x").expect("id");
        assert!(
            ExtensionManifest::new(
                id,
                " ",
                ExtensionVersion::new(0, 1, 0),
                ExtensionCategory::Unknown,
                ExtensionProvenance::new(ExtensionLicenseKind::Apache2)
            )
            .is_err()
        )
    }
    #[test]
    fn manifest_default_lifecycle_discovered() {
        let id = ExtensionId::new("x").expect("id");
        let m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        assert_eq!(m.lifecycle, ExtensionLifecycleState::Discovered)
    }
    #[test]
    fn manifest_default_sandbox_safe() {
        let id = ExtensionId::new("x").expect("id");
        let m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        assert!(m.sandbox.is_safe_default())
    }
    #[test]
    fn manifest_without_execution_contract_requires_review() {
        let m = ExtensionManifest::new(
            ExtensionId::new("x").expect("id"),
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        assert!(m.requires_review())
    }
    #[test]
    fn manifest_with_metadata_execution_contract_can_avoid_review() {
        let m = ExtensionManifest::new(
            ExtensionId::new("x").expect("id"),
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest")
        .with_execution_contract(ExtensionExecutionContract::metadata_only());
        assert!(!m.requires_review())
    }
    #[test]
    fn manifest_with_host_access_requires_review_without_effects() {
        let m = ExtensionManifest::new(
            ExtensionId::new("x").expect("id"),
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest")
        .with_execution_contract(ExtensionExecutionContract::metadata_only())
        .with_sandbox(SandboxPolicy::metadata_only().allow_network(true));
        assert!(m.requires_review());
        assert_eq!(
            m.effect_execution_admission_status(),
            "denied_by_default_host_access"
        );
        assert_eq!(
            m.review_reason_codes(),
            vec!["sandbox_host_access_requested"]
        );
    }
    #[test]
    fn manifest_runtime_review_classifies_non_builtin_runtimes() {
        let m = ExtensionManifest::new(
            ExtensionId::new("runtime.rust").expect("id"),
            "runtime",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::ScalarUdf,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest")
        .with_execution_contract(ExtensionExecutionContract::metadata_only())
        .with_runtime(UdfRuntimeKind::RustNative);
        assert!(m.requires_review());
        assert!(m.runtime_requires_review());
        assert_eq!(
            m.runtime_admission_status(),
            "blocked_rust_native_requires_abi_sandbox_certificate"
        );
        assert_eq!(
            m.effect_execution_admission_status(),
            "denied_by_default_runtime"
        );
        assert_eq!(
            m.review_reason_codes(),
            vec!["runtime_requires_sandbox_or_bridge_review"]
        );
    }
    #[test]
    fn manifest_review_reasons_classify_sandbox_policy_blockers() {
        let none_sandbox = SandboxPolicy {
            kind: SandboxPolicyKind::None,
            allow_filesystem: false,
            allow_network: false,
            allow_environment: false,
            allow_secret_access: false,
            max_memory_bytes: None,
            max_runtime_millis: None,
        };
        let m = ExtensionManifest::new(
            ExtensionId::new("sandbox.none").expect("id"),
            "sandbox",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest")
        .with_execution_contract(ExtensionExecutionContract::metadata_only())
        .with_sandbox(none_sandbox.clone());
        assert_eq!(
            m.review_reason_codes(),
            vec!["sandbox_policy_none_declared"]
        );

        let m = m.with_sandbox(SandboxPolicy {
            kind: SandboxPolicyKind::Unsupported,
            ..none_sandbox
        });
        assert_eq!(
            m.review_reason_codes(),
            vec!["unsupported_sandbox_policy_declared"]
        );
    }
    #[test]
    fn manifest_with_effect_requires_review() {
        let id = ExtensionId::new("x").expect("id");
        let mut m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        m.add_effect(ExtensionEffectDeclaration::new(
            ExternalEffectKind::ApiRead,
            EffectLevel::ExternalRead,
        ));
        assert!(m.requires_review())
    }
    #[test]
    fn report_metadata_only_code_not_executed() {
        let id = ExtensionId::new("x").expect("id");
        let m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        let r = ExtensionInspectionReport::metadata_only(m);
        assert!(!r.code_executed)
    }
    #[test]
    fn manifest_requiring_review_is_not_usable() {
        let mut m = ExtensionManifest::new(
            ExtensionId::new("ext.review").expect("id"),
            "ReviewExt",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Connector,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        m.add_effect(ExtensionEffectDeclaration::new(
            ExternalEffectKind::ApiRead,
            EffectLevel::ExternalRead,
        ));
        assert!(m.requires_review());
        assert!(!m.is_usable());
    }

    #[test]
    fn requires_review_inspection_is_not_error() {
        let manifest = ExtensionManifest::new(
            ExtensionId::new("ext.inspect").expect("id"),
            "Inspect",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Connector,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        let report = ExtensionInspectionReport::requires_review(manifest, "manual review");
        assert!(!report.has_errors());
    }
    #[test]
    fn report_unsupported_has_errors_and_no_fallback() {
        let id = ExtensionId::new("x").expect("id");
        let m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
        )
        .expect("manifest");
        let r = ExtensionInspectionReport::unsupported(m, "feature", "reason");
        assert!(r.has_errors());
        assert!(!r.diagnostics[0].fallback.attempted)
    }
    #[test]
    fn snapshot_empty_zero() {
        assert_eq!(ExtensionRegistrySnapshot::empty().extension_count(), 0)
    }
    #[test]
    fn snapshot_counts() {
        let mut s = ExtensionRegistrySnapshot::empty();
        let id = ExtensionId::new("x").expect("id");
        let mut m = ExtensionManifest::new(
            id,
            "x",
            ExtensionVersion::new(0, 1, 0),
            ExtensionCategory::Unknown,
            ExtensionProvenance::new(ExtensionLicenseKind::Unknown),
        )
        .expect("manifest");
        m = m.with_lifecycle(ExtensionLifecycleState::Validated);
        s.add_manifest(m);
        assert_eq!(s.usable_count(), 0);
        assert_eq!(s.requires_review_count(), 1)
    }

    #[test]
    fn extension_manifest_effect_capability_matrix_blocks_runtime_and_effects() {
        let matrix = ExtensionManifestEffectCapabilityMatrix::report_only();
        assert_eq!(
            matrix.schema_version,
            "shardloom.extension_manifest_effect_capability_matrix.v1"
        );
        assert_eq!(matrix.claim_gate_status, "not_claim_grade");
        assert!(matrix.all_runtime_blocked());
        assert!(matrix.all_external_effects_blocked());
        assert!(matrix.row_order().contains(&"metadata_only_manifest"));
        assert!(matrix.row_order().contains(&"python_udf_extension"));
        assert!(matrix.row_order().contains(&"api_llm_effect_provider"));
        assert!(
            matrix
                .row_order()
                .contains(&"object_store_provider_extension")
        );
        assert!(matrix.rows.iter().all(|row| !row.runtime_execution));
        assert!(matrix.rows.iter().all(|row| !row.extension_code_executed));
        assert!(matrix.rows.iter().all(|row| !row.external_effect_executed));
        assert!(
            matrix
                .rows
                .iter()
                .all(|row| row.claim_boundary.contains("no")
                    || row.claim_boundary.contains("remain"))
        );
    }

    #[test]
    fn plugin_abi_udf_sandbox_blocker_blocks_loading_and_udfs() {
        let report = plan_plugin_abi_udf_sandbox_blocker();
        assert_eq!(
            report.schema_version,
            "shardloom.plugin_abi_udf_sandbox_blocker.v1"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.support_status, "report_only");
        assert!(report.sandbox_evidence_required);
        assert!(report.all_plugin_runtime_blocked());
        for row_id in [
            "abi_contract_inventory",
            "dynamic_library_loading",
            "rust_native_udf",
            "wasm_udf",
            "python_udf",
            "sandbox_evidence_binding",
            "unsupported_diagnostics",
        ] {
            assert!(report.row_order().contains(&row_id), "missing {row_id}");
        }
        assert!(report.required_evidence().iter().any(|evidence| {
            evidence.contains("sandbox_policy") || evidence.contains("sandbox_governance_gate")
        }));
        assert!(
            report
                .blocker_ids()
                .contains(&"gar-0023-a.dynamic_loading_blocked")
        );
        for row in &report.rows {
            assert!(!row.dynamic_loading_performed);
            assert!(!row.extension_code_executed);
            assert!(!row.udf_execution_performed);
            assert!(!row.sandbox_enforced);
            assert!(!row.permission_policy_enforced);
            assert!(!row.runtime_execution);
            assert!(!row.external_effect_executed);
            assert!(!row.credential_resolution_performed);
            assert!(!row.network_probe_performed);
            assert!(!row.dependency_expansion_allowed);
            assert!(!row.fallback_attempted);
            assert!(!row.external_engine_invoked);
            assert!(row.claim_boundary.contains("no") || row.claim_boundary.contains("remain"));
        }
    }

    #[test]
    fn deterministic_scalar_udf_fixture_doubles_nullable_ints_without_effects() {
        let report =
            run_deterministic_scalar_udf_fixture(&[Some(3), None, Some(-4)]).expect("fixture");
        assert_eq!(
            report.schema_version,
            "shardloom.deterministic_scalar_udf_fixture.v1"
        );
        assert_eq!(report.udf_id, "sl_fixture_double_i64");
        assert_eq!(
            report.runtime_kind,
            UdfRuntimeKind::BuiltinDeterministicFixture
        );
        assert_eq!(report.output_values, vec![Some(6), None, Some(-8)]);
        assert_eq!(report.output_values_summary(), "6,null,-8");
        assert!(report.overflow_policy_enforced);
        assert!(!report.overflow_blocked);
        assert_eq!(report.claim_gate_status, "fixture_smoke_only");
        assert!(report.no_fallback_invariant_holds());
    }

    #[test]
    fn deterministic_scalar_udf_fixture_blocks_overflow() {
        let error = run_deterministic_scalar_udf_fixture(&[Some(i64::MAX)])
            .expect_err("overflow is blocked");
        assert!(error.message().contains("overflow blocked"));
    }

    #[test]
    fn deterministic_embedding_vector_fixture_returns_nearest_neighbor_without_effects() {
        let texts = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let report = run_deterministic_embedding_vector_fixture(&texts, "beta").expect("fixture");
        assert_eq!(
            report.schema_version,
            "shardloom.deterministic_embedding_vector_fixture.v1"
        );
        assert_eq!(report.fixture_id, "sl_fixture_hash_embedding_vector");
        assert_eq!(report.output_dtype, "fixed_size_list<int64,4>");
        assert_eq!(report.embedding_model_id, "sl_fixture_hash_embedding_v1");
        assert_eq!(report.vector_index_kind, "local_bruteforce_l2_fixture");
        assert_eq!(report.metric, "squared_l2");
        assert_eq!(report.dimension, 4);
        assert_eq!(report.input_row_count, 3);
        assert_eq!(report.vector_row_count, 3);
        assert_eq!(report.nearest_index, 1);
        assert_eq!(report.nearest_text, "beta");
        assert_eq!(report.nearest_distance_squared, 0);
        assert_eq!(report.claim_gate_status, "fixture_smoke_only");
        assert!(report.no_fallback_invariant_holds());
    }

    #[test]
    fn deterministic_embedding_vector_fixture_rejects_empty_inputs() {
        let error = run_deterministic_embedding_vector_fixture(&[], "beta")
            .expect_err("empty input is blocked");
        assert!(error.message().contains("requires at least one input text"));
        let error = run_deterministic_embedding_vector_fixture(&[String::new()], "beta")
            .expect_err("empty text is blocked");
        assert!(error.message().contains("must not be empty"));
    }
}
