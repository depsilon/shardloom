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

fn summarize_optional_i64_values(values: &[Option<i64>]) -> String {
    values
        .iter()
        .map(|value| value.map_or_else(|| "null".to_string(), |v| v.to_string()))
        .collect::<Vec<_>>()
        .join(",")
}

fn fnv64_digest_text(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
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
    fn metadata_policy_safe() {
        assert!(SandboxPolicy::metadata_only().is_safe_default())
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
}
