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
        false
    }
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
            && !self.provenance.requires_review()
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
        out.diagnostics.push(Diagnostic::configuration_error(
            "extension_inspection",
            reason.into(),
            "Review extension provenance and permissions.",
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
}
