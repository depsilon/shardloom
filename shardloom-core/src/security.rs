//! Security, secrets, governance, and agent-safety planning skeleton.
//!
//! This module defines domain types for planning and reporting only.
//! It does not resolve secrets/credentials or execute external effects.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::struct_excessive_bools
)]

use crate::{Diagnostic, DiagnosticCode, ObservedField, Result, ShardLoomError};
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn validate_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must not be empty"
        )));
    }
    Ok(())
}

/// Stable identifier for a secret reference. Stores only a reference, never a secret value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRefId(String);
impl SecretRefId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("secret reference id", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderKind {
    Environment,
    FileReference,
    ExternalSecretManager,
    CloudIam,
    UserPrompt,
    Disabled,
    Unknown,
}
impl SecretProviderKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Environment => "environment",
            Self::FileReference => "file_reference",
            Self::ExternalSecretManager => "external_secret_manager",
            Self::CloudIam => "cloud_iam",
            Self::UserPrompt => "user_prompt",
            Self::Disabled => "disabled",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_runtime_resolution(&self) -> bool {
        matches!(
            self,
            Self::Environment
                | Self::FileReference
                | Self::ExternalSecretManager
                | Self::CloudIam
                | Self::UserPrompt
        )
    }
}

/// Secret reference metadata, never raw secret material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRef {
    pub id: SecretRefId,
    pub provider: SecretProviderKind,
    pub label: String,
}
impl SecretRef {
    pub fn new(
        id: SecretRefId,
        provider: SecretProviderKind,
        label: impl Into<String>,
    ) -> Result<Self> {
        let label = label.into();
        validate_non_empty("secret label", &label)?;
        Ok(Self {
            id,
            provider,
            label,
        })
    }
    #[must_use]
    pub fn redacted_summary(&self) -> String {
        format!(
            "secret_ref(id={}, provider={}, label={})",
            self.id.as_str(),
            self.provider.as_str(),
            self.label
        )
    }
    pub fn safe_field(&self) -> ObservedField {
        ObservedField::secret(format!("secret_ref:{}", self.id.as_str())).expect("non-empty")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialScopeKind {
    ObjectStoreRead,
    ObjectStoreWrite,
    LocalFileRead,
    LocalFileWrite,
    ApiRead,
    ApiWrite,
    LlmCall,
    EmbeddingGeneration,
    VectorSearch,
    CatalogRead,
    CatalogWrite,
    UdfExecution,
    PluginExecution,
    Unknown,
}
impl CredentialScopeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ObjectStoreRead => "object_store_read",
            Self::ObjectStoreWrite => "object_store_write",
            Self::LocalFileRead => "local_file_read",
            Self::LocalFileWrite => "local_file_write",
            Self::ApiRead => "api_read",
            Self::ApiWrite => "api_write",
            Self::LlmCall => "llm_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::CatalogRead => "catalog_read",
            Self::CatalogWrite => "catalog_write",
            Self::UdfExecution => "udf_execution",
            Self::PluginExecution => "plugin_execution",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_write_or_mutating(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreWrite
                | Self::LocalFileWrite
                | Self::ApiWrite
                | Self::CatalogWrite
                | Self::PluginExecution
                | Self::UdfExecution
        )
    }
    #[must_use]
    pub const fn is_external_effect(&self) -> bool {
        matches!(
            self,
            Self::ApiRead
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::CatalogRead
                | Self::CatalogWrite
                | Self::UdfExecution
                | Self::PluginExecution
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialScope {
    pub kind: CredentialScopeKind,
    pub resource_pattern: String,
    pub secret: Option<SecretRef>,
    pub read_allowed: bool,
    pub write_allowed: bool,
}
impl CredentialScope {
    pub fn new(kind: CredentialScopeKind, resource_pattern: impl Into<String>) -> Result<Self> {
        let rp = resource_pattern.into();
        validate_non_empty("credential resource pattern", &rp)?;
        Ok(Self {
            kind,
            resource_pattern: rp,
            secret: None,
            read_allowed: false,
            write_allowed: false,
        })
    }
    #[must_use]
    pub fn with_secret(mut self, secret: SecretRef) -> Self {
        self.secret = Some(secret);
        self
    }
    #[must_use]
    pub fn allow_read(mut self, value: bool) -> Self {
        self.read_allowed = value;
        self
    }
    #[must_use]
    pub fn allow_write(mut self, value: bool) -> Self {
        self.write_allowed = value;
        self
    }
    #[must_use]
    pub const fn is_write_capable(&self) -> bool {
        self.write_allowed || self.kind.is_write_or_mutating()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "credential_scope(kind={}, resource_pattern={}, read_allowed={}, write_allowed={}, secret_ref={})",
            self.kind.as_str(),
            self.resource_pattern,
            self.read_allowed,
            self.write_allowed,
            self.secret.as_ref().map_or("none", |s| s.id.as_str())
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionKind {
    ReadMetadata,
    ReadData,
    WriteTemporaryOutput,
    CommitOutput,
    DeleteTemporaryFiles,
    AccessNetwork,
    AccessFilesystem,
    AccessSecret,
    CallLlm,
    CallApi,
    GenerateEmbeddings,
    ExternalWrite,
    ExecuteUdf,
    ExecutePlugin,
    ExportCompatibilityOutput,
    Unsupported,
}
impl PermissionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReadMetadata => "read_metadata",
            Self::ReadData => "read_data",
            Self::WriteTemporaryOutput => "write_temporary_output",
            Self::CommitOutput => "commit_output",
            Self::DeleteTemporaryFiles => "delete_temporary_files",
            Self::AccessNetwork => "access_network",
            Self::AccessFilesystem => "access_filesystem",
            Self::AccessSecret => "access_secret",
            Self::CallLlm => "call_llm",
            Self::CallApi => "call_api",
            Self::GenerateEmbeddings => "generate_embeddings",
            Self::ExternalWrite => "external_write",
            Self::ExecuteUdf => "execute_udf",
            Self::ExecutePlugin => "execute_plugin",
            Self::ExportCompatibilityOutput => "export_compatibility_output",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        matches!(
            self,
            Self::CallLlm
                | Self::CallApi
                | Self::GenerateEmbeddings
                | Self::ExternalWrite
                | Self::ExecuteUdf
                | Self::ExecutePlugin
                | Self::AccessNetwork
                | Self::AccessSecret
        )
    }
    #[must_use]
    pub const fn is_destructive_or_mutating(&self) -> bool {
        matches!(
            self,
            Self::CommitOutput
                | Self::DeleteTemporaryFiles
                | Self::ExternalWrite
                | Self::WriteTemporaryOutput
                | Self::ExecutePlugin
                | Self::ExecuteUdf
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionStatus {
    Granted,
    Denied,
    RequiresApproval,
    RequiresConfiguration,
    Disabled,
    Unknown,
}
impl PermissionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::RequiresApproval => "requires_approval",
            Self::RequiresConfiguration => "requires_configuration",
            Self::Disabled => "disabled",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        matches!(self, Self::Granted)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequirement {
    pub kind: PermissionKind,
    pub status: PermissionStatus,
    pub reason: String,
}
impl PermissionRequirement {
    fn new(kind: PermissionKind, status: PermissionStatus, reason: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            reason: reason.into(),
        }
    }
    pub fn granted(kind: PermissionKind, reason: impl Into<String>) -> Self {
        Self::new(kind, PermissionStatus::Granted, reason)
    }
    pub fn denied(kind: PermissionKind, reason: impl Into<String>) -> Self {
        Self::new(kind, PermissionStatus::Denied, reason)
    }
    pub fn requires_approval(kind: PermissionKind, reason: impl Into<String>) -> Self {
        Self::new(kind, PermissionStatus::RequiresApproval, reason)
    }
    pub fn requires_configuration(kind: PermissionKind, reason: impl Into<String>) -> Self {
        Self::new(kind, PermissionStatus::RequiresConfiguration, reason)
    }
    pub fn disabled(kind: PermissionKind, reason: impl Into<String>) -> Self {
        Self::new(kind, PermissionStatus::Disabled, reason)
    }
    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        self.status.allows_execution()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "permission(kind={}, status={}, reason={})",
            self.kind.as_str(),
            self.status.as_str(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalEffectKind {
    None,
    ObjectStoreWrite,
    LocalFileWrite,
    ApiRead,
    ApiWrite,
    LlmCall,
    EmbeddingGeneration,
    VectorSearch,
    CatalogRead,
    CatalogWrite,
    ExternalWorkflowTrigger,
    UdfExecution,
    PluginExecution,
    Unknown,
}
impl ExternalEffectKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ObjectStoreWrite => "object_store_write",
            Self::LocalFileWrite => "local_file_write",
            Self::ApiRead => "api_read",
            Self::ApiWrite => "api_write",
            Self::LlmCall => "llm_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::CatalogRead => "catalog_read",
            Self::CatalogWrite => "catalog_write",
            Self::ExternalWorkflowTrigger => "external_workflow_trigger",
            Self::UdfExecution => "udf_execution",
            Self::PluginExecution => "plugin_execution",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        !matches!(self, Self::None)
    }
    #[must_use]
    pub const fn is_write_or_mutation(&self) -> bool {
        matches!(
            self,
            Self::ApiWrite
                | Self::ObjectStoreWrite
                | Self::LocalFileWrite
                | Self::CatalogWrite
                | Self::ExternalWorkflowTrigger
                | Self::UdfExecution
                | Self::PluginExecution
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectPolicy {
    pub kind: ExternalEffectKind,
    pub enabled: bool,
    pub dry_run_allowed: bool,
    pub requires_approval: bool,
    pub requires_idempotency: bool,
    pub max_calls: Option<u64>,
}
impl ExternalEffectPolicy {
    pub const fn disabled(kind: ExternalEffectKind) -> Self {
        Self {
            kind,
            enabled: false,
            dry_run_allowed: false,
            requires_approval: false,
            requires_idempotency: false,
            max_calls: None,
        }
    }
    pub const fn enabled_read_only(kind: ExternalEffectKind) -> Self {
        Self {
            kind,
            enabled: true,
            dry_run_allowed: true,
            requires_approval: false,
            requires_idempotency: false,
            max_calls: None,
        }
    }
    pub const fn requires_approval(kind: ExternalEffectKind) -> Self {
        Self {
            kind,
            enabled: false,
            dry_run_allowed: false,
            requires_approval: true,
            requires_idempotency: true,
            max_calls: None,
        }
    }
    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        self.enabled
            && !self.requires_approval
            && !matches!(
                self.kind,
                ExternalEffectKind::None | ExternalEffectKind::Unknown
            )
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "effect_policy(kind={}, enabled={}, dry_run_allowed={}, requires_approval={})",
            self.kind.as_str(),
            self.enabled,
            self.dry_run_allowed,
            self.requires_approval
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRunSafety {
    Safe,
    RequiresMetadataOnly,
    UnsafeWouldReadData,
    UnsafeWouldWrite,
    UnsafeWouldCallExternalEffect,
    Unsupported,
    Unknown,
}
impl DryRunSafety {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::RequiresMetadataOnly => "requires_metadata_only",
            Self::UnsafeWouldReadData => "unsafe_would_read_data",
            Self::UnsafeWouldWrite => "unsafe_would_write",
            Self::UnsafeWouldCallExternalEffect => "unsafe_would_call_external_effect",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_safe(&self) -> bool {
        matches!(self, Self::Safe | Self::RequiresMetadataOnly)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalRequirement {
    None,
    Required,
    Granted,
    Denied,
    Expired,
    Unknown,
}
impl ApprovalRequirement {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Required => "required",
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Expired => "expired",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        matches!(self, Self::None | Self::Granted)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactionPolicyKind {
    None,
    SecretsOnly,
    SensitiveValues,
    FieldNamesOnly,
    OmitPayloads,
    Strict,
}
impl RedactionPolicyKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SecretsOnly => "secrets_only",
            Self::SensitiveValues => "sensitive_values",
            Self::FieldNamesOnly => "field_names_only",
            Self::OmitPayloads => "omit_payloads",
            Self::Strict => "strict",
        }
    }
    #[must_use]
    pub const fn redacts_sensitive_values(&self) -> bool {
        matches!(
            self,
            Self::SensitiveValues | Self::FieldNamesOnly | Self::OmitPayloads | Self::Strict
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionPolicy {
    pub kind: RedactionPolicyKind,
    pub redact_prompts: bool,
    pub redact_payloads: bool,
    pub redact_paths: bool,
}
impl RedactionPolicy {
    #[must_use]
    pub const fn default_safe() -> Self {
        Self {
            kind: RedactionPolicyKind::SecretsOnly,
            redact_prompts: false,
            redact_payloads: false,
            redact_paths: false,
        }
    }
    #[must_use]
    pub const fn strict() -> Self {
        Self {
            kind: RedactionPolicyKind::Strict,
            redact_prompts: true,
            redact_payloads: true,
            redact_paths: true,
        }
    }
    #[must_use]
    pub const fn allows_raw_sensitive_values(&self) -> bool {
        !self.kind.redacts_sensitive_values()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "redaction(kind={}, redact_prompts={}, redact_payloads={}, redact_paths={})",
            self.kind.as_str(),
            self.redact_prompts,
            self.redact_payloads,
            self.redact_paths
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSensitivity {
    Public,
    Internal,
    Confidential,
    Pii,
    Secret,
    Unknown,
}
impl DataSensitivity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Pii => "pii",
            Self::Secret => "secret",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_policy(&self) -> bool {
        matches!(
            self,
            Self::Confidential | Self::Pii | Self::Secret | Self::Unknown
        )
    }
    #[must_use]
    pub const fn requires_redaction(&self) -> bool {
        matches!(self, Self::Pii | Self::Secret | Self::Unknown)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensitiveField {
    pub field_name: String,
    pub sensitivity: DataSensitivity,
    pub redaction: RedactionPolicyKind,
}
impl SensitiveField {
    pub fn new(field_name: impl Into<String>, sensitivity: DataSensitivity) -> Result<Self> {
        let field_name = field_name.into();
        validate_non_empty("sensitive field name", &field_name)?;
        Ok(Self {
            field_name,
            sensitivity,
            redaction: RedactionPolicyKind::SecretsOnly,
        })
    }
    #[must_use]
    pub fn with_redaction(mut self, redaction: RedactionPolicyKind) -> Self {
        self.redaction = redaction;
        self
    }
    #[must_use]
    pub const fn requires_redaction(&self) -> bool {
        self.sensitivity.requires_redaction() || self.redaction.redacts_sensitive_values()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "sensitive_field(name={}, sensitivity={}, redaction={})",
            self.field_name,
            self.sensitivity.as_str(),
            self.redaction.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSafetyMode {
    HumanOnly,
    AgentDryRunOnly,
    AgentPlanOnly,
    AgentReadOnly,
    AgentWithApproval,
    AgentDisabled,
}
impl AgentSafetyMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::HumanOnly => "human_only",
            Self::AgentDryRunOnly => "agent_dry_run_only",
            Self::AgentPlanOnly => "agent_plan_only",
            Self::AgentReadOnly => "agent_read_only",
            Self::AgentWithApproval => "agent_with_approval",
            Self::AgentDisabled => "agent_disabled",
        }
    }
    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        matches!(
            self,
            Self::HumanOnly | Self::AgentReadOnly | Self::AgentWithApproval
        )
    }
    #[must_use]
    pub const fn allows_external_effects(&self) -> bool {
        matches!(self, Self::AgentWithApproval)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicyStatus {
    Planned,
    Enforced,
    DiagnosticOnly,
    Disabled,
    Unsupported,
}
impl SecurityPolicyStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Enforced => "enforced",
            Self::DiagnosticOnly => "diagnostic_only",
            Self::Disabled => "disabled",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecurityPlan {
    pub status: SecurityPolicyStatus,
    pub agent_mode: AgentSafetyMode,
    pub redaction: RedactionPolicy,
    pub permissions: Vec<PermissionRequirement>,
    pub credential_scopes: Vec<CredentialScope>,
    pub effect_policies: Vec<ExternalEffectPolicy>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub diagnostics: Vec<Diagnostic>,
}
impl SecurityPlan {
    #[must_use]
    pub fn default_safe() -> Self {
        Self {
            status: SecurityPolicyStatus::DiagnosticOnly,
            agent_mode: AgentSafetyMode::AgentDryRunOnly,
            redaction: RedactionPolicy::strict(),
            permissions: vec![],
            credential_scopes: vec![],
            effect_policies: vec![ExternalEffectPolicy::disabled(ExternalEffectKind::Unknown)],
            sensitive_fields: vec![],
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn diagnostic_only() -> Self {
        Self::default_safe()
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut s = Self::default_safe();
        s.status = SecurityPolicyStatus::Unsupported;
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEffect,
            feature,
            reason,
            Some("Use security planning/reporting skeletons only.".to_string()),
        ));
        s
    }
    pub fn add_permission(&mut self, permission: PermissionRequirement) {
        self.permissions.push(permission);
    }
    pub fn add_credential_scope(&mut self, scope: CredentialScope) {
        self.credential_scopes.push(scope);
    }
    pub fn add_effect_policy(&mut self, policy: ExternalEffectPolicy) {
        self.effect_policies.push(policy);
    }
    pub fn add_sensitive_field(&mut self, field: SensitiveField) {
        self.sensitive_fields.push(field);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn allows_external_effects(&self) -> bool {
        self.agent_mode.allows_external_effects()
            && self
                .effect_policies
                .iter()
                .any(ExternalEffectPolicy::allows_execution)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "security plan status: {}\nagent mode: {}\nredaction: {}\nexternal effects allowed: {}\nfallback execution: disabled\nexecution: not performed\nplanning/reporting skeleton only",
            self.status.as_str(),
            self.agent_mode.as_str(),
            self.redaction.summary(),
            self.allows_external_effects()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditActionKind {
    PlanCreated,
    DryRunRequested,
    PermissionChecked,
    CredentialScopeReferenced,
    ExternalEffectPlanned,
    ExternalEffectSkipped,
    ApprovalRequired,
    OutputWritePlanned,
    CommitPlanned,
    UnsupportedFeature,
}
impl AuditActionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanCreated => "plan_created",
            Self::DryRunRequested => "dry_run_requested",
            Self::PermissionChecked => "permission_checked",
            Self::CredentialScopeReferenced => "credential_scope_referenced",
            Self::ExternalEffectPlanned => "external_effect_planned",
            Self::ExternalEffectSkipped => "external_effect_skipped",
            Self::ApprovalRequired => "approval_required",
            Self::OutputWritePlanned => "output_write_planned",
            Self::CommitPlanned => "commit_planned",
            Self::UnsupportedFeature => "unsupported_feature",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    pub action: AuditActionKind,
    pub subject: String,
    pub dry_run: bool,
    pub external_effect: Option<ExternalEffectKind>,
    pub diagnostics: Vec<Diagnostic>,
}
impl AuditRecord {
    pub fn new(action: AuditActionKind, subject: impl Into<String>) -> Result<Self> {
        let subject = subject.into();
        validate_non_empty("audit subject", &subject)?;
        Ok(Self {
            action,
            subject,
            dry_run: true,
            external_effect: None,
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn dry_run(mut self, value: bool) -> Self {
        self.dry_run = value;
        self
    }
    #[must_use]
    pub fn with_external_effect(mut self, effect: ExternalEffectKind) -> Self {
        self.external_effect = Some(effect);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "audit(action={}, subject={}, dry_run={}, effect={})",
            self.action.as_str(),
            self.subject,
            self.dry_run,
            self.external_effect.map_or("none", |e| e.as_str())
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecurityReport {
    pub plan: SecurityPlan,
    pub audit_records: Vec<AuditRecord>,
    pub diagnostics: Vec<Diagnostic>,
}
impl SecurityReport {
    #[must_use]
    pub fn from_plan(plan: SecurityPlan) -> Self {
        Self {
            plan,
            audit_records: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_audit_record(&mut self, record: AuditRecord) {
        self.audit_records.push(record);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.plan.has_errors()
            || self.audit_records.iter().any(AuditRecord::has_errors)
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{}\naudit records: {}\nfallback execution: disabled\nsecurity report skeleton only; no effects executed",
            self.plan.to_human_text(),
            self.audit_records.len()
        )
    }
}

fn lexically_normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn path_has_parent_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn invalid_security_input(feature: &str, reason: impl Into<String>) -> Diagnostic {
    Diagnostic::invalid_input(
        feature,
        reason,
        "Reject the input or route it through an explicit ShardLoom safety policy.",
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInputSafetyReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub input_kind: String,
    pub input_path: String,
    pub canonicalized_path: String,
    pub path_traversal_checked: bool,
    pub symlink_policy: String,
    pub hardlink_policy: String,
    pub max_size_policy: String,
    pub max_depth_policy: String,
    pub invalid_utf8_policy: String,
    pub malformed_input_policy: String,
    pub panic_free_status: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl RuntimeInputSafetyReport {
    #[must_use]
    pub fn deterministic_block(
        input_kind: impl Into<String>,
        input_path: impl AsRef<Path>,
        reason: impl Into<String>,
    ) -> Self {
        let input_kind = input_kind.into();
        let input_path_ref = input_path.as_ref();
        let canonicalized_path = lexically_normalize_path(input_path_ref);
        Self {
            schema_version: "shardloom.runtime_input_safety_report.v1",
            report_id: format!("runtime_input_safety.{input_kind}"),
            input_kind,
            input_path: input_path_ref.display().to_string(),
            canonicalized_path: canonicalized_path.display().to_string(),
            path_traversal_checked: true,
            symlink_policy: "not_followed_without_explicit_workspace_policy".to_string(),
            hardlink_policy: "not_trusted_without_explicit_workspace_policy".to_string(),
            max_size_policy: "bounded_by_calling_surface_or_blocked".to_string(),
            max_depth_policy: "bounded_by_calling_surface_or_blocked".to_string(),
            invalid_utf8_policy: "deterministic_invalid_input_diagnostic".to_string(),
            malformed_input_policy: "deterministic_invalid_or_unsupported_diagnostic".to_string(),
            panic_free_status: "diagnostic_no_panic".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
            diagnostics: vec![invalid_security_input(
                "runtime_input_safety",
                reason.into(),
            )],
        }
    }

    #[must_use]
    pub fn malformed_without_panic(
        input_kind: impl Into<String>,
        input_path: impl AsRef<Path>,
    ) -> Self {
        Self::deterministic_block(
            input_kind,
            input_path,
            "malformed input is blocked with a deterministic diagnostic before release claims",
        )
    }

    #[must_use]
    pub fn invalid_utf8_without_panic(input_path: impl AsRef<Path>) -> Self {
        Self::deterministic_block(
            "text",
            input_path,
            "invalid UTF-8 is blocked with a deterministic diagnostic",
        )
    }

    #[must_use]
    pub fn oversized_or_deeply_nested_blocker(
        input_kind: impl Into<String>,
        input_path: impl AsRef<Path>,
    ) -> Self {
        Self::deterministic_block(
            input_kind,
            input_path,
            "oversized or deeply nested input requires bounded parser evidence before release claims",
        )
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self.diagnostics.iter().all(|d| !d.fallback.attempted)
    }

    #[must_use]
    pub fn deterministic_no_panic(&self) -> bool {
        self.panic_free_status == "diagnostic_no_panic" && !self.diagnostics.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePathSafetyReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub workspace_root: String,
    pub canonical_workspace_root: String,
    pub requested_output_path: String,
    pub canonical_output_path: String,
    pub path_traversal_checked: bool,
    pub within_workspace: bool,
    pub symlink_followed: bool,
    pub symlink_policy: String,
    pub hardlink_policy: String,
    pub overwrite_policy: String,
    pub cleanup_policy: String,
    pub rollback_policy: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl WorkspacePathSafetyReport {
    #[must_use]
    pub fn evaluate(
        workspace_root: impl AsRef<Path>,
        requested_output_path: impl AsRef<Path>,
    ) -> Self {
        let workspace_root = workspace_root.as_ref();
        let requested_output_path = requested_output_path.as_ref();
        let canonical_workspace_root = lexically_normalize_path(workspace_root);
        let requested_absolute = if requested_output_path.is_absolute() {
            requested_output_path.to_path_buf()
        } else {
            canonical_workspace_root.join(requested_output_path)
        };
        let canonical_output_path = lexically_normalize_path(&requested_absolute);
        let parent_traversal = path_has_parent_traversal(requested_output_path);
        let within_workspace =
            !parent_traversal && canonical_output_path.starts_with(&canonical_workspace_root);
        let mut diagnostics = Vec::new();
        if parent_traversal {
            diagnostics.push(invalid_security_input(
                "workspace_path_safety",
                "output path contains parent-directory traversal",
            ));
        }
        if !canonical_output_path.starts_with(&canonical_workspace_root) {
            diagnostics.push(invalid_security_input(
                "workspace_path_safety",
                "output path resolves outside the declared workspace",
            ));
        }

        Self {
            schema_version: "shardloom.workspace_path_safety_report.v1",
            report_id: "workspace_path_safety.local_output".to_string(),
            workspace_root: workspace_root.display().to_string(),
            canonical_workspace_root: canonical_workspace_root.display().to_string(),
            requested_output_path: requested_output_path.display().to_string(),
            canonical_output_path: canonical_output_path.display().to_string(),
            path_traversal_checked: true,
            within_workspace,
            symlink_followed: false,
            symlink_policy: "do_not_follow_untrusted_output_symlinks".to_string(),
            hardlink_policy: "block_or_require_explicit_policy_for_untrusted_hardlinks".to_string(),
            overwrite_policy: "explicit_only".to_string(),
            cleanup_policy: "caller_workspace_scoped_cleanup".to_string(),
            rollback_policy: "deterministic_cleanup_or_blocked_before_commit".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn accepted(&self) -> bool {
        self.within_workspace
            && self.path_traversal_checked
            && !self.symlink_followed
            && self.diagnostics.is_empty()
            && self.no_fallback_invariant_holds()
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self.diagnostics.iter().all(|d| !d.fallback.attempted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSafeLocalWritePlan {
    pub path_safety_report: WorkspacePathSafetyReport,
    pub target_path: PathBuf,
    pub parent_path: PathBuf,
    pub target_existed_before: bool,
    pub overwrite_allowed: bool,
    pub hardlink_count: Option<u64>,
}

impl WorkspaceSafeLocalWritePlan {
    #[must_use]
    pub fn accepted(&self) -> bool {
        self.path_safety_report.accepted()
    }
}

const WORKSPACE_SAFE_LOCAL_STAGING_BUFFER_BYTES: usize = 256 * 1024;

/// Same-directory staging writer for workspace-safe local outputs.
///
/// Callers use this through [`write_workspace_safe_bytes_with_producer`] when an
/// output provider can stream bytes directly into the staged artifact. The
/// writer updates the final output digest and byte count as bytes are accepted,
/// buffers staged filesystem writes to reduce provider write syscall pressure,
/// and then the surrounding helper flushes before commit while preserving
/// `ShardLoom`'s workspace path checks, symlink-race checks, same-directory
/// staging, atomic commit, rollback, and no-fallback evidence.
#[derive(Debug)]
pub struct WorkspaceSafeLocalStagingWriter {
    file: std::io::BufWriter<fs::File>,
    bytes_written: u64,
    digest: Fnv64Digest,
}

impl WorkspaceSafeLocalStagingWriter {
    #[must_use]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    #[must_use]
    pub fn output_digest(&self) -> String {
        self.digest.output_digest()
    }
}

impl std::io::Write for WorkspaceSafeLocalStagingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        self.digest.update(&buf[..written]);
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(written).map_err(|error| {
                std::io::Error::other(format!(
                    "workspace-safe local output byte count overflow: {error}"
                ))
            })?)
            .ok_or_else(|| {
                std::io::Error::other("workspace-safe local output byte count overflow")
            })?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSafeLocalWriteReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub operation_label: String,
    pub path_safety_report: WorkspacePathSafetyReport,
    pub target_path: PathBuf,
    pub staging_path: PathBuf,
    pub target_existed_before: bool,
    pub overwrite_allowed: bool,
    pub overwrite_performed: bool,
    pub hardlink_count: Option<u64>,
    pub commit_mode: String,
    pub commit_status: String,
    pub cleanup_status: String,
    pub rollback_status: String,
    pub bytes_written: u64,
    pub output_digest: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl WorkspaceSafeLocalWriteReport {
    #[must_use]
    pub fn evidence_fields(&self, prefix: &str) -> Vec<(String, String)> {
        let p = prefix.trim_matches('_');
        let key = |name: &str| -> String {
            if p.is_empty() {
                name.to_string()
            } else {
                format!("{p}_{name}")
            }
        };
        vec![
            (key("workspace_path_safety_status"), "enforced".to_string()),
            (
                key("workspace_path_safety_schema_version"),
                self.path_safety_report.schema_version.to_string(),
            ),
            (
                key("workspace_root"),
                self.path_safety_report.workspace_root.clone(),
            ),
            (
                key("canonical_workspace_root"),
                self.path_safety_report.canonical_workspace_root.clone(),
            ),
            (
                key("requested_output_path"),
                self.path_safety_report.requested_output_path.clone(),
            ),
            (
                key("canonical_output_path"),
                self.path_safety_report.canonical_output_path.clone(),
            ),
            (
                key("within_workspace"),
                self.path_safety_report.within_workspace.to_string(),
            ),
            (
                key("path_traversal_checked"),
                self.path_safety_report.path_traversal_checked.to_string(),
            ),
            (
                key("symlink_followed"),
                self.path_safety_report.symlink_followed.to_string(),
            ),
            (
                key("symlink_policy"),
                self.path_safety_report.symlink_policy.clone(),
            ),
            (
                key("hardlink_policy"),
                self.path_safety_report.hardlink_policy.clone(),
            ),
            (
                key("hardlink_count"),
                self.hardlink_count.map_or_else(
                    || "unknown_or_not_applicable".to_string(),
                    |n| n.to_string(),
                ),
            ),
            (key("overwrite_allowed"), self.overwrite_allowed.to_string()),
            (
                key("overwrite_performed"),
                self.overwrite_performed.to_string(),
            ),
            (
                key("target_existed_before"),
                self.target_existed_before.to_string(),
            ),
            (key("staging_path"), self.staging_path.display().to_string()),
            (key("commit_mode"), self.commit_mode.clone()),
            (key("commit_status"), self.commit_status.clone()),
            (key("cleanup_status"), self.cleanup_status.clone()),
            (key("rollback_status"), self.rollback_status.clone()),
            (key("bytes_written"), self.bytes_written.to_string()),
            (key("output_digest"), self.output_digest.clone()),
            (
                key("fallback_attempted"),
                self.fallback_attempted.to_string(),
            ),
            (
                key("external_engine_invoked"),
                self.external_engine_invoked.to_string(),
            ),
        ]
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self.path_safety_report.no_fallback_invariant_holds()
    }
}

pub fn infer_local_output_workspace_root(output_path: impl AsRef<Path>) -> Result<PathBuf> {
    let output_path = output_path.as_ref();
    if !output_path.is_absolute() {
        return std::env::current_dir().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to resolve current directory for workspace-safe local output: {error}; no fallback execution was attempted"
            ))
        });
    }

    let mut candidate = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(output_path)
        .to_path_buf();
    loop {
        if candidate.exists() {
            return Ok(candidate);
        }
        let Some(parent) = candidate.parent() else {
            return std::env::current_dir().map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to resolve current directory for workspace-safe local output: {error}; no fallback execution was attempted"
                ))
            });
        };
        if parent == candidate {
            return Ok(candidate);
        }
        candidate = parent.to_path_buf();
    }
}

pub fn plan_workspace_safe_local_output(
    workspace_root: impl AsRef<Path>,
    requested_output_path: impl AsRef<Path>,
    allow_overwrite: bool,
) -> Result<WorkspaceSafeLocalWritePlan> {
    let workspace_root = workspace_root.as_ref();
    let requested_output_path = requested_output_path.as_ref();
    let workspace_root_is_symlink = workspace_safe_root_is_symlink(workspace_root)?;
    let canonical_workspace_root = fs::canonicalize(workspace_root).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output root '{}' must already exist and be canonicalizable: {error}; no fallback execution was attempted",
            workspace_root.display()
        ))
    })?;
    let requested_absolute = if requested_output_path.is_absolute() {
        requested_output_path.to_path_buf()
    } else {
        canonical_workspace_root.join(requested_output_path)
    };
    let target_path = canonicalize_local_output_target_path(&requested_absolute)?;
    let parent_path = target_path.parent().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output target '{}' has no parent directory; no fallback execution was attempted",
            target_path.display()
        ))
    })?;
    let mut report = WorkspacePathSafetyReport::evaluate(&canonical_workspace_root, &target_path);
    report.workspace_root = workspace_root.display().to_string();
    report.canonical_workspace_root = canonical_workspace_root.display().to_string();
    report.requested_output_path = requested_output_path.display().to_string();
    report.canonical_output_path = target_path.display().to_string();
    if workspace_root_is_symlink {
        report.symlink_policy =
            "canonical_workspace_root_symlink_allowed_output_symlinks_not_followed".to_string();
    }
    let parent_traversal = path_has_parent_traversal(requested_output_path);
    report.within_workspace =
        !parent_traversal && target_path.starts_with(&canonical_workspace_root);
    if parent_traversal {
        report.diagnostics.push(invalid_security_input(
            "workspace_path_safety",
            "output path contains parent-directory traversal",
        ));
    }
    if !report.within_workspace && report.diagnostics.is_empty() {
        report.diagnostics.push(invalid_security_input(
            "workspace_path_safety",
            "output path resolves outside the declared workspace",
        ));
    }
    if let Some(symlink_path) =
        first_existing_symlink_component(&canonical_workspace_root, &target_path)?
    {
        report.diagnostics.push(invalid_security_input(
            "workspace_path_safety",
            format!(
                "output path crosses symlink component '{}'",
                symlink_path.display()
            ),
        ));
    }

    let parent = parent_path.to_path_buf();
    let target_metadata = fs::symlink_metadata(&target_path).ok();
    let target_existed_before = target_metadata.is_some();
    let observed_hardlink_count;
    if let Some(metadata) = target_metadata.as_ref() {
        if metadata.file_type().is_symlink() {
            report.diagnostics.push(invalid_security_input(
                "workspace_path_safety",
                "output target is a symlink and will not be followed",
            ));
        }
        if metadata.is_dir() {
            report.diagnostics.push(invalid_security_input(
                "workspace_path_safety",
                "output target exists as a directory",
            ));
        }
        if !allow_overwrite {
            report.diagnostics.push(invalid_security_input(
                "workspace_path_safety",
                "output target already exists and overwrite is disabled",
            ));
        }
        observed_hardlink_count = observe_workspace_safe_hardlink_count(metadata, &mut report);
    } else {
        observed_hardlink_count = None;
    }

    if !report.accepted() {
        return Err(workspace_path_safety_error(&report));
    }

    Ok(WorkspaceSafeLocalWritePlan {
        path_safety_report: report,
        target_path,
        parent_path: parent,
        target_existed_before,
        overwrite_allowed: allow_overwrite,
        hardlink_count: observed_hardlink_count,
    })
}

pub fn write_workspace_safe_bytes(
    workspace_root: impl AsRef<Path>,
    requested_output_path: impl AsRef<Path>,
    allow_overwrite: bool,
    operation_label: impl Into<String>,
    content: &[u8],
) -> Result<WorkspaceSafeLocalWriteReport> {
    let ((), report) = write_workspace_safe_bytes_with_producer(
        workspace_root,
        requested_output_path,
        allow_overwrite,
        operation_label,
        |writer| {
            writer.write_all(content).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to write workspace-safe local output bytes: {error}; staging cleanup attempted; no fallback execution was attempted"
                ))
            })
        },
    )?;
    Ok(report)
}

pub fn write_workspace_safe_bytes_with_producer<T>(
    workspace_root: impl AsRef<Path>,
    requested_output_path: impl AsRef<Path>,
    allow_overwrite: bool,
    operation_label: impl Into<String>,
    producer: impl FnOnce(&mut WorkspaceSafeLocalStagingWriter) -> Result<T>,
) -> Result<(T, WorkspaceSafeLocalWriteReport)> {
    write_workspace_safe_bytes_with_validated_producer(
        workspace_root,
        requested_output_path,
        allow_overwrite,
        operation_label,
        producer,
        |_| Ok(()),
    )
}

pub fn write_workspace_safe_bytes_with_validated_producer<T>(
    workspace_root: impl AsRef<Path>,
    requested_output_path: impl AsRef<Path>,
    allow_overwrite: bool,
    operation_label: impl Into<String>,
    producer: impl FnOnce(&mut WorkspaceSafeLocalStagingWriter) -> Result<T>,
    validate_before_commit: impl FnOnce(&T) -> Result<()>,
) -> Result<(T, WorkspaceSafeLocalWriteReport)> {
    let operation_label = operation_label.into();
    let plan =
        plan_workspace_safe_local_output(workspace_root, requested_output_path, allow_overwrite)?;
    reject_workspace_safe_symlink_race(&plan)?;
    fs::create_dir_all(&plan.parent_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create workspace-safe local output directory '{}': {error}; no fallback execution was attempted",
            plan.parent_path.display()
        ))
    })?;
    reject_workspace_safe_symlink_race(&plan)?;

    let (staging_path, mut staging_writer) = create_workspace_safe_staging_writer(&plan)?;
    let producer_output = match producer(&mut staging_writer) {
        Ok(output) => output,
        Err(error) => {
            drop(staging_writer);
            let _ = fs::remove_file(&staging_path);
            return Err(error);
        }
    };
    if let Err(error) = validate_before_commit(&producer_output) {
        drop(staging_writer);
        let _ = fs::remove_file(&staging_path);
        return Err(error);
    }
    let bytes_written = staging_writer.bytes_written();
    let output_digest = staging_writer.output_digest();
    if let Err(error) = staging_writer.flush() {
        drop(staging_writer);
        let _ = fs::remove_file(&staging_path);
        return Err(ShardLoomError::InvalidOperation(format!(
            "failed to flush workspace-safe local output staging file '{}': {error}; staging cleanup attempted; no fallback execution was attempted",
            staging_path.display()
        )));
    }
    drop(staging_writer);
    let (commit_mode, cleanup_status, rollback_status, overwrite_performed) =
        commit_workspace_safe_staging_file(&plan, &staging_path)?;

    Ok((
        producer_output,
        WorkspaceSafeLocalWriteReport {
            schema_version: "shardloom.workspace_safe_local_write_report.v1",
            report_id: "workspace_safe_local_write.local_output".to_string(),
            operation_label,
            path_safety_report: plan.path_safety_report,
            target_path: plan.target_path,
            staging_path,
            target_existed_before: plan.target_existed_before,
            overwrite_allowed: plan.overwrite_allowed,
            overwrite_performed,
            hardlink_count: plan.hardlink_count,
            commit_mode,
            commit_status: "committed".to_string(),
            cleanup_status,
            rollback_status,
            bytes_written,
            output_digest,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    ))
}

fn reject_workspace_safe_symlink_race(plan: &WorkspaceSafeLocalWritePlan) -> Result<()> {
    if let Some(symlink_path) = first_existing_symlink_component(
        Path::new(&plan.path_safety_report.canonical_workspace_root),
        &plan.target_path,
    )? {
        return Err(ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output target '{}' crosses symlink component '{}'; no fallback execution was attempted",
            plan.target_path.display(),
            symlink_path.display()
        )));
    }
    Ok(())
}

fn create_workspace_safe_staging_writer(
    plan: &WorkspaceSafeLocalWritePlan,
) -> Result<(PathBuf, WorkspaceSafeLocalStagingWriter)> {
    let staging_path = unique_sidecar_path(&plan.parent_path, &plan.target_path, "tmp");
    let staging_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create workspace-safe local output staging file '{}': {error}; no fallback execution was attempted",
                staging_path.display()
            ))
        })?;
    Ok((
        staging_path,
        WorkspaceSafeLocalStagingWriter {
            file: std::io::BufWriter::with_capacity(
                WORKSPACE_SAFE_LOCAL_STAGING_BUFFER_BYTES,
                staging_file,
            ),
            bytes_written: 0,
            digest: Fnv64Digest::new(),
        },
    ))
}

fn commit_workspace_safe_staging_file(
    plan: &WorkspaceSafeLocalWritePlan,
    staging_path: &Path,
) -> Result<(String, String, String, bool)> {
    let target_existed_at_commit = plan.target_path.exists();
    if target_existed_at_commit && !plan.overwrite_allowed {
        let _ = fs::remove_file(staging_path);
        return Err(ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output target '{}' appeared before commit and overwrite is disabled; staging cleanup attempted; no fallback execution was attempted",
            plan.target_path.display()
        )));
    }

    if target_existed_at_commit {
        replace_workspace_safe_existing_target(plan, staging_path)
    } else {
        commit_workspace_safe_new_target(plan, staging_path)
    }
}

fn replace_workspace_safe_existing_target(
    plan: &WorkspaceSafeLocalWritePlan,
    staging_path: &Path,
) -> Result<(String, String, String, bool)> {
    let backup_path = unique_sidecar_path(&plan.parent_path, &plan.target_path, "backup");
    fs::rename(&plan.target_path, &backup_path).map_err(|error| {
        let _ = fs::remove_file(staging_path);
        ShardLoomError::InvalidOperation(format!(
            "failed to stage existing workspace-safe local output target '{}' for replacement: {error}; staging cleanup attempted; no fallback execution was attempted",
            plan.target_path.display()
        ))
    })?;
    if let Err(error) = fs::rename(staging_path, &plan.target_path) {
        let rollback = fs::rename(&backup_path, &plan.target_path).is_ok();
        let _ = fs::remove_file(staging_path);
        return Err(ShardLoomError::InvalidOperation(format!(
            "failed to commit workspace-safe local output target '{}': {error}; rollback_restored_existing_target={rollback}; no fallback execution was attempted",
            plan.target_path.display()
        )));
    }
    let backup_cleanup_status = if fs::remove_file(&backup_path).is_ok() {
        "backup_removed"
    } else {
        "backup_cleanup_failed_or_not_needed"
    };
    Ok((
        "staged_replace_with_backup_same_directory".to_string(),
        "no_staging_artifacts_remaining".to_string(),
        backup_cleanup_status.to_string(),
        true,
    ))
}

fn commit_workspace_safe_new_target(
    plan: &WorkspaceSafeLocalWritePlan,
    staging_path: &Path,
) -> Result<(String, String, String, bool)> {
    fs::rename(staging_path, &plan.target_path).map_err(|error| {
        let _ = fs::remove_file(staging_path);
        ShardLoomError::InvalidOperation(format!(
            "failed to atomically commit workspace-safe local output target '{}': {error}; staging cleanup attempted; no fallback execution was attempted",
            plan.target_path.display()
        ))
    })?;
    Ok((
        "atomic_rename_same_directory".to_string(),
        "no_staging_artifacts_remaining".to_string(),
        "not_required_new_target".to_string(),
        false,
    ))
}

fn workspace_path_safety_error(report: &WorkspacePathSafetyReport) -> ShardLoomError {
    let reasons = report
        .diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.reason.as_deref())
        .collect::<Vec<_>>()
        .join("; ");
    ShardLoomError::InvalidOperation(format!(
        "workspace-safe local output rejected for '{}': {}; no fallback execution was attempted",
        report.requested_output_path,
        if reasons.is_empty() {
            "path safety policy rejected the output"
        } else {
            &reasons
        }
    ))
}

fn workspace_safe_root_is_symlink(workspace_root: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(workspace_root).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output root '{}' must already exist and be inspectable before canonicalization: {error}; no fallback execution was attempted",
            workspace_root.display()
        ))
    })?;
    if !metadata.file_type().is_symlink() && !metadata.is_dir() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output root '{}' must be a directory; no fallback execution was attempted",
            workspace_root.display()
        )));
    }
    Ok(metadata.file_type().is_symlink())
}

fn canonicalize_local_output_target_path(requested_absolute: &Path) -> Result<PathBuf> {
    let parent = requested_absolute.parent().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output target '{}' has no parent directory; no fallback execution was attempted",
            requested_absolute.display()
        ))
    })?;
    let file_name = requested_absolute.file_name().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output target '{}' must include a file name; no fallback execution was attempted",
            requested_absolute.display()
        ))
    })?;
    let canonical_parent = canonicalize_local_output_parent(parent)?;
    Ok(canonical_parent.join(file_name))
}

fn canonicalize_local_output_parent(parent: &Path) -> Result<PathBuf> {
    let mut candidate = parent.to_path_buf();
    let mut missing_components = Vec::<OsString>::new();
    while !candidate.exists() {
        let Some(name) = candidate.file_name() else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "workspace-safe local output parent '{}' has no existing ancestor; no fallback execution was attempted",
                parent.display()
            )));
        };
        missing_components.push(name.to_os_string());
        let Some(next) = candidate.parent() else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "workspace-safe local output parent '{}' has no existing ancestor; no fallback execution was attempted",
                parent.display()
            )));
        };
        candidate = next.to_path_buf();
    }
    let candidate_metadata = fs::symlink_metadata(&candidate).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect workspace-safe local output parent ancestor '{}': {error}; no fallback execution was attempted",
            candidate.display()
        ))
    })?;
    if candidate_metadata.file_type().is_symlink() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "workspace-safe local output parent ancestor '{}' is a symlink and will not be followed; no fallback execution was attempted",
            candidate.display()
        )));
    }
    let mut canonical = fs::canonicalize(&candidate).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to canonicalize workspace-safe local output parent ancestor '{}': {error}; no fallback execution was attempted",
            candidate.display()
        ))
    })?;
    for component in missing_components.iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

fn first_existing_symlink_component(
    canonical_workspace_root: &Path,
    target_path: &Path,
) -> Result<Option<PathBuf>> {
    let mut current = PathBuf::new();
    for component in target_path.components() {
        current.push(component.as_os_str());
        if !current.starts_with(canonical_workspace_root) && current != canonical_workspace_root {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(Some(current)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "failed to inspect workspace-safe local output path component '{}': {error}; no fallback execution was attempted",
                    current.display()
                )));
            }
        }
    }
    Ok(None)
}

fn unique_sidecar_path(parent: &Path, target_path: &Path, kind: &str) -> PathBuf {
    let file_name = target_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("output");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    for attempt in 0..1024_u16 {
        let candidate = parent.join(format!(
            ".{file_name}.shardloom-{kind}-{}-{nanos}-{attempt}",
            std::process::id()
        ));
        if !candidate.exists() {
            return candidate;
        }
    }
    parent.join(format!(
        ".{file_name}.shardloom-{kind}-{}-{nanos}-fallback",
        std::process::id()
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fnv64Digest {
    state: u64,
}

impl Fnv64Digest {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self {
            state: Self::OFFSET,
        }
    }

    fn update(&mut self, value: &[u8]) {
        for byte in value {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(Self::PRIME);
        }
    }

    fn output_digest(self) -> String {
        format!("fnv64:{:016x}", self.state)
    }
}

#[cfg(test)]
fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut digest = Fnv64Digest::new();
    digest.update(value);
    digest.output_digest()
}

#[cfg(unix)]
fn observe_workspace_safe_hardlink_count(
    metadata: &fs::Metadata,
    report: &mut WorkspacePathSafetyReport,
) -> Option<u64> {
    use std::os::unix::fs::MetadataExt as _;
    if !metadata.is_file() {
        return None;
    }
    let count = metadata.nlink();
    if count > 1 {
        report.diagnostics.push(invalid_security_input(
            "workspace_path_safety",
            "output target has multiple hardlinks and overwrite is blocked",
        ));
    }
    Some(count)
}

#[cfg(not(unix))]
fn observe_workspace_safe_hardlink_count(
    _metadata: &fs::Metadata,
    _report: &mut WorkspacePathSafetyReport,
) -> Option<u64> {
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceArtifactSafetyReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub artifact_id: String,
    pub contains_credentials: bool,
    pub contains_paths: bool,
    pub contains_user_values: bool,
    pub contains_query_text: bool,
    pub contains_schema_names: bool,
    pub contains_samples: bool,
    pub redaction_policy: String,
    pub retention_policy: String,
    pub export_allowed: bool,
    pub agent_visible: bool,
    pub redacted_preview: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl EvidenceArtifactSafetyReport {
    #[must_use]
    pub fn inspect_text(
        artifact_id: impl Into<String>,
        text: impl AsRef<str>,
        redaction_policy: &RedactionPolicy,
    ) -> Self {
        let artifact_id = artifact_id.into();
        let raw = text.as_ref();
        let redacted_preview = redact_credential_like_values(raw);
        let contains_credentials = redacted_preview != raw;
        let mut diagnostics = Vec::new();
        if contains_credentials {
            diagnostics.push(invalid_security_input(
                "evidence_artifact_safety",
                "credential-like value detected and redacted; export remains blocked until reviewed",
            ));
        }
        Self {
            schema_version: "shardloom.evidence_artifact_safety_report.v1",
            report_id: format!("evidence_artifact_safety.{artifact_id}"),
            artifact_id,
            contains_credentials,
            contains_paths: raw.contains(":\\") || raw.contains("://") || raw.contains('/'),
            contains_user_values: !raw.trim().is_empty(),
            contains_query_text: raw.to_ascii_lowercase().contains("select "),
            contains_schema_names: raw.to_ascii_lowercase().contains("schema"),
            contains_samples: raw.to_ascii_lowercase().contains("sample"),
            redaction_policy: redaction_policy.summary(),
            retention_policy: "release_gate_review_required_before_export".to_string(),
            export_allowed: !contains_credentials
                && redaction_policy.kind != RedactionPolicyKind::None,
            agent_visible: !contains_credentials && redaction_policy.redact_payloads,
            redacted_preview,
            fallback_attempted: false,
            external_engine_invoked: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn no_raw_credential_preview(&self, forbidden: &str) -> bool {
        !self.redacted_preview.contains(forbidden)
    }

    #[must_use]
    pub fn no_fallback_invariant_holds(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self.diagnostics.iter().all(|d| !d.fallback.attempted)
    }
}

#[must_use]
pub fn redact_credential_like_values(input: &str) -> String {
    let mut output = Vec::new();
    let mut redact_next = false;
    for token in input.split_whitespace() {
        let lower = token.to_ascii_lowercase();
        if redact_next {
            if matches!(lower.as_str(), "bearer" | "basic") {
                output.push(token.to_string());
                redact_next = true;
            } else {
                output.push("<redacted>".to_string());
                redact_next = false;
            }
            continue;
        }

        if matches!(lower.as_str(), "bearer" | "basic") {
            output.push(token.to_string());
            redact_next = true;
            continue;
        }
        if lower == "authorization:" || lower == "auth:" {
            output.push(token.to_string());
            redact_next = true;
            continue;
        }

        let redacted = [
            "password=",
            "password:",
            "passwd=",
            "passwd:",
            "token=",
            "token:",
            "api_key=",
            "api_key:",
            "apikey=",
            "apikey:",
            "secret=",
            "secret:",
            "authorization=",
            "authorization:",
        ]
        .iter()
        .find_map(|prefix| {
            lower
                .starts_with(prefix)
                .then(|| format!("{}<redacted>", &token[..prefix.len()]))
        });

        output.push(redacted.unwrap_or_else(|| token.to_string()));
    }
    output.join(" ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityGovernanceEvidenceArea {
    CredentialReference,
    PermissionBoundary,
    RedactionPolicy,
    AuditTrail,
    ExternalEffect,
    DestructiveOperation,
    DataEgress,
    AgentPolicy,
}
impl SecurityGovernanceEvidenceArea {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CredentialReference => "credential_reference",
            Self::PermissionBoundary => "permission_boundary",
            Self::RedactionPolicy => "redaction_policy",
            Self::AuditTrail => "audit_trail",
            Self::ExternalEffect => "external_effect",
            Self::DestructiveOperation => "destructive_operation",
            Self::DataEgress => "data_egress",
            Self::AgentPolicy => "agent_policy",
        }
    }

    #[must_use]
    pub const fn required() -> &'static [Self] {
        &[
            Self::CredentialReference,
            Self::PermissionBoundary,
            Self::RedactionPolicy,
            Self::AuditTrail,
            Self::ExternalEffect,
            Self::DestructiveOperation,
            Self::DataEgress,
            Self::AgentPolicy,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityGovernanceEvidenceStatus {
    ReportOnly,
    BlockedUntilPolicy,
    BlockedUntilRuntimeEvidence,
    Enforced,
}
impl SecurityGovernanceEvidenceStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::BlockedUntilPolicy => "blocked_until_policy",
            Self::BlockedUntilRuntimeEvidence => "blocked_until_runtime_evidence",
            Self::Enforced => "enforced",
        }
    }

    #[must_use]
    pub const fn allows_effectful_claims(&self) -> bool {
        matches!(self, Self::Enforced)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityGovernanceEvidenceEntry {
    pub area: SecurityGovernanceEvidenceArea,
    pub status: SecurityGovernanceEvidenceStatus,
    pub required_for_claims: &'static str,
    pub default_policy: &'static str,
    pub evidence_field: &'static str,
    pub effectful_claim_allowed: bool,
}
impl SecurityGovernanceEvidenceEntry {
    #[must_use]
    pub const fn report_only(
        area: SecurityGovernanceEvidenceArea,
        required_for_claims: &'static str,
        default_policy: &'static str,
        evidence_field: &'static str,
    ) -> Self {
        Self {
            area,
            status: SecurityGovernanceEvidenceStatus::ReportOnly,
            required_for_claims,
            default_policy,
            evidence_field,
            effectful_claim_allowed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityGovernanceEvidenceGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<SecurityGovernanceEvidenceEntry>,
    pub effectful_features_default_denied: bool,
    pub dry_run_required_without_policy: bool,
    pub credential_references_only: bool,
    pub credentials_resolved: bool,
    pub secrets_loaded: bool,
    pub redaction_required: bool,
    pub audit_required: bool,
    pub external_effects_executed: bool,
    pub external_effect_claims_allowed: bool,
    pub destructive_operations_allowed: bool,
    pub data_egress_allowed: bool,
    pub object_store_claims_blocked: bool,
    pub api_server_claims_blocked: bool,
    pub llm_media_udf_claims_blocked: bool,
    pub agent_execute_write_cancel_allowed: bool,
    pub runtime_execution_performed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl SecurityGovernanceEvidenceGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.security_governance_evidence_gate.v1",
            report_id: "cross_cutting.security_governance_evidence_gate",
            entries: vec![
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::CredentialReference,
                    "object-store, API, model, catalog, plugin, UDF, and server claims",
                    "references_only_no_secret_resolution",
                    "credential_reference_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::PermissionBoundary,
                    "read, write, commit, network, model, plugin, UDF, and export claims",
                    "deny_unconfigured_permissions",
                    "permission_boundary_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::RedactionPolicy,
                    "diagnostic, certificate, trace, profile, artifact, and agent-visible claims",
                    "strict_redaction_required",
                    "redaction_policy_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::AuditTrail,
                    "effectful execution, writes, exports, API calls, and model-call claims",
                    "audit_required_before_effects",
                    "audit_trail_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::ExternalEffect,
                    "API, LLM, embedding, vector, UDF, plugin, workflow, and catalog claims",
                    "dry_run_or_denied_without_policy",
                    "external_effect_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::DestructiveOperation,
                    "commit, delete, overwrite, external-write, plugin, and UDF claims",
                    "denied_until_explicit_destructive_policy",
                    "destructive_operation_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::DataEgress,
                    "object-store write, compatibility export, API/model call, and server claims",
                    "egress_denied_until_policy_and_redaction",
                    "data_egress_evidence_present",
                ),
                SecurityGovernanceEvidenceEntry::report_only(
                    SecurityGovernanceEvidenceArea::AgentPolicy,
                    "agent-facing execute, write, cancel, export, and external-effect claims",
                    "agent_dry_run_only_by_default",
                    "agent_policy_evidence_present",
                ),
            ],
            effectful_features_default_denied: true,
            dry_run_required_without_policy: true,
            credential_references_only: true,
            credentials_resolved: false,
            secrets_loaded: false,
            redaction_required: true,
            audit_required: true,
            external_effects_executed: false,
            external_effect_claims_allowed: false,
            destructive_operations_allowed: false,
            data_egress_allowed: false,
            object_store_claims_blocked: true,
            api_server_claims_blocked: true,
            llm_media_udf_claims_blocked: true,
            agent_execute_write_cancel_allowed: false,
            runtime_execution_performed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn evidence_area_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn report_only_area_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == SecurityGovernanceEvidenceStatus::ReportOnly)
            .count()
    }

    #[must_use]
    pub fn effectful_claim_allowed_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.effectful_claim_allowed || entry.status.allows_effectful_claims())
            .count()
    }

    #[must_use]
    pub fn area_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.area.as_str())
            .collect()
    }

    #[must_use]
    pub fn all_evidence_surfaces_present(&self) -> bool {
        self.all_required_evidence_areas_present()
            && self.entries.iter().all(|entry| {
                !entry.required_for_claims.is_empty()
                    && !entry.default_policy.is_empty()
                    && !entry.evidence_field.is_empty()
            })
    }

    #[must_use]
    pub fn all_required_evidence_areas_present(&self) -> bool {
        self.missing_required_area_count() == 0
    }

    #[must_use]
    pub fn missing_required_area_count(&self) -> usize {
        SecurityGovernanceEvidenceArea::required()
            .iter()
            .filter(|area| !self.entries.iter().any(|entry| entry.area == **area))
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.effectful_features_default_denied
            && self.dry_run_required_without_policy
            && self.credential_references_only
            && !self.credentials_resolved
            && !self.secrets_loaded
            && self.redaction_required
            && self.audit_required
            && !self.external_effects_executed
            && !self.external_effect_claims_allowed
            && !self.destructive_operations_allowed
            && !self.data_egress_allowed
            && self.object_store_claims_blocked
            && self.api_server_claims_blocked
            && self.llm_media_udf_claims_blocked
            && !self.agent_execute_write_cancel_allowed
            && !self.runtime_execution_performed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self.effectful_claim_allowed_count() == 0
    }

    #[must_use]
    pub fn claims_blocked_by_default(&self) -> bool {
        self.object_store_claims_blocked
            && self.api_server_claims_blocked
            && self.llm_media_udf_claims_blocked
            && !self.external_effect_claims_allowed
            && !self.destructive_operations_allowed
            && !self.data_egress_allowed
            && !self.agent_execute_write_cancel_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.all_evidence_surfaces_present()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        out.push_str("security/governance evidence gate\n");
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "effectful features default denied: {}",
            self.effectful_features_default_denied
        );
        let _ = writeln!(
            out,
            "claims blocked by default: {}",
            self.claims_blocked_by_default()
        );
        let _ = writeln!(
            out,
            "runtime execution performed: {}",
            self.runtime_execution_performed
        );
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        out.push_str("evidence areas:\n");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] default_policy={} claims_allowed={}",
                entry.area.as_str(),
                entry.status.as_str(),
                entry.default_policy,
                entry.effectful_claim_allowed
            );
        }
        out
    }
}

#[must_use]
pub fn plan_security_governance_evidence_gate() -> SecurityGovernanceEvidenceGateReport {
    SecurityGovernanceEvidenceGateReport::planning_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialPolicyEnforcementGateRow {
    pub row_id: &'static str,
    pub lifecycle_surface: &'static str,
    pub support_status: &'static str,
    pub default_policy: &'static str,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub user_visible_surface: &'static str,
    pub credential_resolution_performed: bool,
    pub secret_loading_performed: bool,
    pub secret_value_materialized: bool,
    pub runtime_permission_check_enforced: bool,
    pub workspace_policy_enforced: bool,
    pub redaction_required: bool,
    pub audit_required: bool,
    pub network_probe_performed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl CredentialPolicyEnforcementGateRow {
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    const fn new(
        row_id: &'static str,
        lifecycle_surface: &'static str,
        support_status: &'static str,
        default_policy: &'static str,
        blocker_id: &'static str,
        diagnostic_code: &'static str,
        required_evidence: &'static str,
        user_visible_surface: &'static str,
        runtime_permission_check_enforced: bool,
        workspace_policy_enforced: bool,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            lifecycle_surface,
            support_status,
            default_policy,
            blocker_id,
            diagnostic_code,
            required_evidence,
            user_visible_surface,
            credential_resolution_performed: false,
            secret_loading_performed: false,
            secret_value_materialized: false,
            runtime_permission_check_enforced,
            workspace_policy_enforced,
            redaction_required: true,
            audit_required: true,
            network_probe_performed: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialPolicyEnforcementGateReport {
    pub schema_version: &'static str,
    pub gate_id: &'static str,
    pub docs_ref: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<CredentialPolicyEnforcementGateRow>,
    pub credential_references_only: bool,
    pub credential_resolution_performed: bool,
    pub secret_loading_performed: bool,
    pub secret_value_materialized: bool,
    pub runtime_permission_checks_enforced: bool,
    pub workspace_policy_enforced: bool,
    pub production_policy_runtime_supported: bool,
    pub redaction_required: bool,
    pub audit_required: bool,
    pub network_probe_performed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl CredentialPolicyEnforcementGateReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.credential_policy_enforcement_gate.v1",
            gate_id: "gar-0019-a.credential_lifecycle_policy_enforcement_gate",
            docs_ref: "docs/architecture/credential-policy-enforcement-gate.md",
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                CredentialPolicyEnforcementGateRow::new(
                    "credential_reference_inventory",
                    "credential_reference",
                    "report_only",
                    "reference_metadata_only",
                    "none_reference_inventory",
                    "SL_CREDENTIAL_REFERENCE_ONLY",
                    "secret_ref_metadata,provider_kind,scope_kind,redaction_policy,no_fallback_evidence",
                    "security-governance-evidence-gate,capabilities security-governance",
                    false,
                    false,
                    "Credential references may be inventoried as metadata only; ShardLoom does not resolve or load secret values.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "secret_loading",
                    "secret_value_loading",
                    "blocked",
                    "denied_until_explicit_policy_and_runtime_evidence",
                    "gar-0019-a.secret_loading_blocked",
                    "SL_CREDENTIAL_POLICY_BLOCKED",
                    "secret_provider_policy,redaction_policy,audit_trail,permission_check,execution_certificate,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "Secret loading remains blocked; no secret value is materialized into diagnostics, plans, evidence, or runtime state.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "environment_secret_provider",
                    "environment",
                    "blocked",
                    "references_only_no_env_read",
                    "gar-0019-a.environment_secret_provider_blocked",
                    "SL_CREDENTIAL_POLICY_BLOCKED",
                    "environment_provider_policy,allowlist,redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "Environment secret providers remain reference-only; ShardLoom does not read environment secret values.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "file_secret_provider",
                    "file_reference",
                    "blocked",
                    "references_only_no_file_read",
                    "gar-0019-a.file_secret_provider_blocked",
                    "SL_CREDENTIAL_POLICY_BLOCKED",
                    "path_safety_policy,workspace_policy,redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "File secret providers remain reference-only; ShardLoom does not read secret files.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "external_secret_manager_provider",
                    "external_secret_manager",
                    "blocked",
                    "network_and_credentials_denied",
                    "gar-0019-a.external_secret_manager_provider_blocked",
                    "SL_CREDENTIAL_POLICY_BLOCKED",
                    "provider_policy,network_policy,credential_policy,redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "External secret managers remain blocked; no provider client, network probe, or credential exchange is performed.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "cloud_iam_provider",
                    "cloud_iam",
                    "blocked",
                    "network_and_iam_denied",
                    "gar-0019-a.cloud_iam_provider_blocked",
                    "SL_CREDENTIAL_POLICY_BLOCKED",
                    "iam_policy,provider_probe_policy,network_policy,redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "Cloud IAM credential resolution remains blocked; no token exchange, metadata service call, or provider probe is performed.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "workspace_policy",
                    "workspace_policy",
                    "report_only",
                    "declared_policy_only",
                    "gar-0019-a.workspace_policy_runtime_enforcement_missing",
                    "SL_POLICY_ENFORCEMENT_REPORT_ONLY",
                    "workspace_root,path_safety_policy,symlink_policy,write_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate,workspace path safety reports",
                    false,
                    false,
                    "Workspace policy can be reported for current path-safety surfaces; production runtime policy enforcement remains incomplete.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "runtime_permission_check",
                    "runtime_permission",
                    "blocked",
                    "deny_without_runtime_permission_evidence",
                    "gar-0019-a.runtime_permission_check_blocked",
                    "SL_POLICY_ENFORCEMENT_REPORT_ONLY",
                    "permission_manifest,capability_check,audit_trail,execution_certificate,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    false,
                    false,
                    "Runtime permission checks remain blocked for production claims until every effectful operation records enforcement evidence.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "redaction_policy",
                    "redaction",
                    "report_only",
                    "strict_redaction_required",
                    "gar-0019-a.redaction_policy_runtime_enforcement_missing",
                    "SL_POLICY_ENFORCEMENT_REPORT_ONLY",
                    "redaction_policy,artifact_safety_report,diagnostic_snapshot,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate,release security gate",
                    false,
                    false,
                    "Strict redaction is required for claimable surfaces; this row does not claim a production redaction runtime.",
                ),
                CredentialPolicyEnforcementGateRow::new(
                    "unsupported_diagnostics",
                    "diagnostic",
                    "report_only",
                    "deterministic_unsupported_without_effects",
                    "none_diagnostic_only",
                    "SL_CREDENTIAL_POLICY_UNSUPPORTED",
                    "diagnostic_code,blocker_id,claim_boundary,no_fallback_evidence",
                    "security-governance-evidence-gate,capabilities security-governance",
                    false,
                    false,
                    "Unsupported credential paths must emit deterministic diagnostics without resolving credentials, loading secrets, or invoking fallback.",
                ),
            ],
            credential_references_only: true,
            credential_resolution_performed: false,
            secret_loading_performed: false,
            secret_value_materialized: false,
            runtime_permission_checks_enforced: false,
            workspace_policy_enforced: false,
            production_policy_runtime_supported: false,
            redaction_required: true,
            audit_required: true,
            network_probe_performed: false,
            external_effect_executed: false,
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
    pub fn all_credential_runtime_blocked(&self) -> bool {
        self.credential_references_only
            && !self.credential_resolution_performed
            && !self.secret_loading_performed
            && !self.secret_value_materialized
            && !self.production_policy_runtime_supported
            && !self.network_probe_performed
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self.rows.iter().all(|row| {
                !row.credential_resolution_performed
                    && !row.secret_loading_performed
                    && !row.secret_value_materialized
                    && !row.network_probe_performed
                    && !row.external_effect_executed
                    && !row.fallback_attempted
                    && !row.external_engine_invoked
            })
    }
}

#[must_use]
pub fn plan_credential_policy_enforcement_gate() -> CredentialPolicyEnforcementGateReport {
    CredentialPolicyEnforcementGateReport::report_only()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxGovernanceReadinessRow {
    pub row_id: &'static str,
    pub readiness_surface: &'static str,
    pub support_status: &'static str,
    pub default_policy: &'static str,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub user_visible_surface: &'static str,
    pub sandbox_enforced: bool,
    pub filesystem_access_allowed: bool,
    pub network_access_allowed: bool,
    pub environment_access_allowed: bool,
    pub secret_access_allowed: bool,
    pub process_execution_allowed: bool,
    pub resource_limits_enforced: bool,
    pub timeout_enforced: bool,
    pub audit_log_emitted: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

impl SandboxGovernanceReadinessRow {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        row_id: &'static str,
        readiness_surface: &'static str,
        support_status: &'static str,
        default_policy: &'static str,
        blocker_id: &'static str,
        diagnostic_code: &'static str,
        required_evidence: &'static str,
        user_visible_surface: &'static str,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            readiness_surface,
            support_status,
            default_policy,
            blocker_id,
            diagnostic_code,
            required_evidence,
            user_visible_surface,
            sandbox_enforced: false,
            filesystem_access_allowed: false,
            network_access_allowed: false,
            environment_access_allowed: false,
            secret_access_allowed: false,
            process_execution_allowed: false,
            resource_limits_enforced: false,
            timeout_enforced: false,
            audit_log_emitted: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxGovernanceReadinessReport {
    pub schema_version: &'static str,
    pub gate_id: &'static str,
    pub docs_ref: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<SandboxGovernanceReadinessRow>,
    pub deny_by_default: bool,
    pub sandbox_runtime_supported: bool,
    pub sandbox_process_spawned: bool,
    pub extension_code_executed: bool,
    pub udf_code_executed: bool,
    pub filesystem_access_allowed: bool,
    pub network_access_allowed: bool,
    pub environment_access_allowed: bool,
    pub secret_access_allowed: bool,
    pub process_execution_allowed: bool,
    pub resource_limits_enforced: bool,
    pub timeout_enforced: bool,
    pub audit_required: bool,
    pub audit_log_runtime_supported: bool,
    pub deterministic_unsupported_diagnostics: bool,
    pub production_governance_runtime_supported: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl SandboxGovernanceReadinessReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.sandbox_governance_readiness_gate.v1",
            gate_id: "gar-0019-b.sandbox_governance_runtime_readiness",
            docs_ref: "docs/architecture/sandbox-governance-runtime-readiness.md",
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                SandboxGovernanceReadinessRow::new(
                    "sandbox_profile_inventory",
                    "sandbox_profile",
                    "report_only",
                    "profile_metadata_only",
                    "none_profile_inventory",
                    "SL_SANDBOX_PROFILE_REPORT_ONLY",
                    "sandbox_profile,permission_manifest,effect_budget,credential_policy,no_fallback_evidence",
                    "security-governance-evidence-gate,capabilities security-governance",
                    "Sandbox profiles may be inventoried as metadata only; no sandbox runtime or plugin/UDF execution is claimed.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "filesystem_permission",
                    "filesystem_access",
                    "blocked",
                    "deny_host_filesystem_access",
                    "gar-0019-b.filesystem_permission_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "filesystem_allowlist,workspace_policy,path_safety,write_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Filesystem access for sandboxed code remains blocked until isolation and audit evidence exist.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "network_permission",
                    "network_access",
                    "blocked",
                    "deny_network_access",
                    "gar-0019-b.network_permission_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "network_policy,egress_allowlist,credential_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Network access remains blocked; no network probe, egress, or external service call is performed.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "environment_access",
                    "environment_access",
                    "blocked",
                    "deny_environment_access",
                    "gar-0019-b.environment_access_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "environment_allowlist,secret_redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Environment access remains blocked; sandboxed code cannot read environment variables or secret-bearing process state.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "secret_access",
                    "secret_access",
                    "blocked",
                    "deny_secret_access",
                    "gar-0019-b.secret_access_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "credential_policy,secret_provider_policy,redaction_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Secret access remains blocked and inherits the GAR-0019-A credential policy gate.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "process_execution",
                    "process_execution",
                    "blocked",
                    "deny_child_process_execution",
                    "gar-0019-b.process_execution_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "process_policy,plugin_provenance,license_review,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Process execution remains blocked; no child process, plugin binary, or external engine is invoked.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "resource_limits",
                    "resource_limits",
                    "blocked",
                    "deny_until_resource_enforcement",
                    "gar-0019-b.resource_limits_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "memory_budget,cpu_budget,spill_policy,timeout_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Resource-limit claims remain blocked until memory, CPU, and spill enforcement evidence exists.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "execution_timeout",
                    "execution_timeout",
                    "blocked",
                    "deny_until_timeout_enforcement",
                    "gar-0019-b.timeout_enforcement_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "timeout_policy,cancellation_policy,retry_policy,audit_trail,no_fallback_evidence",
                    "security-governance-evidence-gate",
                    "Timeout enforcement remains blocked until cancellation and cleanup evidence exists.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "audit_log",
                    "audit_log",
                    "report_only",
                    "audit_required_before_execution",
                    "gar-0019-b.audit_log_runtime_missing",
                    "SL_SANDBOX_AUDIT_REPORT_ONLY",
                    "audit_schema,principal,capability,target,outcome,redaction_policy,no_fallback_evidence",
                    "security-governance-evidence-gate,release security gate",
                    "Audit schema posture is report-only; runtime audit emission for sandboxed effects is not claimed.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "dependency_isolation",
                    "dependency_isolation",
                    "blocked",
                    "deny_until_dependency_isolation",
                    "gar-0019-b.dependency_isolation_blocked",
                    "SL_SANDBOX_POLICY_BLOCKED",
                    "dependency_manifest,license_provenance,supply_chain_attestation,isolation_policy,no_fallback_evidence",
                    "security-governance-evidence-gate,release security gate",
                    "Dependency isolation remains blocked until provenance, license, and runtime isolation evidence exists.",
                ),
                SandboxGovernanceReadinessRow::new(
                    "unsupported_diagnostics",
                    "diagnostics",
                    "report_only",
                    "deterministic_unsupported_without_execution",
                    "none_diagnostic_only",
                    "SL_SANDBOX_POLICY_UNSUPPORTED",
                    "diagnostic_code,blocker_id,claim_boundary,no_fallback_evidence",
                    "security-governance-evidence-gate,capabilities security-governance",
                    "Unsupported sandbox/governance paths must emit deterministic diagnostics without executing code, effects, or fallback engines.",
                ),
            ],
            deny_by_default: true,
            sandbox_runtime_supported: false,
            sandbox_process_spawned: false,
            extension_code_executed: false,
            udf_code_executed: false,
            filesystem_access_allowed: false,
            network_access_allowed: false,
            environment_access_allowed: false,
            secret_access_allowed: false,
            process_execution_allowed: false,
            resource_limits_enforced: false,
            timeout_enforced: false,
            audit_required: true,
            audit_log_runtime_supported: false,
            deterministic_unsupported_diagnostics: true,
            production_governance_runtime_supported: false,
            external_effect_executed: false,
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
    pub fn all_sandbox_runtime_blocked(&self) -> bool {
        self.deny_by_default
            && !self.sandbox_runtime_supported
            && !self.sandbox_process_spawned
            && !self.extension_code_executed
            && !self.udf_code_executed
            && !self.filesystem_access_allowed
            && !self.network_access_allowed
            && !self.environment_access_allowed
            && !self.secret_access_allowed
            && !self.process_execution_allowed
            && !self.resource_limits_enforced
            && !self.timeout_enforced
            && self.audit_required
            && !self.audit_log_runtime_supported
            && self.deterministic_unsupported_diagnostics
            && !self.production_governance_runtime_supported
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self.rows.iter().all(|row| {
                !row.sandbox_enforced
                    && !row.filesystem_access_allowed
                    && !row.network_access_allowed
                    && !row.environment_access_allowed
                    && !row.secret_access_allowed
                    && !row.process_execution_allowed
                    && !row.resource_limits_enforced
                    && !row.timeout_enforced
                    && !row.audit_log_emitted
                    && !row.external_effect_executed
                    && !row.fallback_attempted
                    && !row.external_engine_invoked
            })
    }
}

#[must_use]
pub fn plan_sandbox_governance_readiness_gate() -> SandboxGovernanceReadinessReport {
    SandboxGovernanceReadinessReport::report_only()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_path_fixture_root(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("shardloom_{name}"))
    }

    #[test]
    fn secret_ref_id_rejects_empty() {
        assert!(SecretRefId::new("  ").is_err());
    }
    #[test]
    fn secret_provider_environment_requires_runtime_resolution() {
        assert!(SecretProviderKind::Environment.requires_runtime_resolution());
    }
    #[test]
    fn secret_ref_redacted_summary_no_secret_values() {
        let id = SecretRefId::new("s1").unwrap();
        let s = SecretRef::new(id, SecretProviderKind::Environment, "token").unwrap();
        let summary = s.redacted_summary();
        assert!(!summary.contains("actual-secret"));
    }
    #[test]
    fn credential_scope_kind_api_write_mutating() {
        assert!(CredentialScopeKind::ApiWrite.is_write_or_mutating());
    }
    #[test]
    fn credential_scope_rejects_empty_resource() {
        assert!(CredentialScope::new(CredentialScopeKind::ApiRead, " ").is_err());
    }
    #[test]
    fn credential_scope_summary_no_secret_values() {
        let id = SecretRefId::new("s1").unwrap();
        let s = SecretRef::new(id, SecretProviderKind::Environment, "token").unwrap();
        let c = CredentialScope::new(CredentialScopeKind::ApiRead, "svc://x")
            .unwrap()
            .with_secret(s);
        let summary = c.summary();
        assert!(!summary.contains("actual-secret"));
    }
    #[test]
    fn permission_call_llm_effectful() {
        assert!(PermissionKind::CallLlm.is_effectful());
    }
    #[test]
    fn permission_external_write_mutating() {
        assert!(PermissionKind::ExternalWrite.is_destructive_or_mutating());
    }
    #[test]
    fn permission_status_granted_allows() {
        assert!(PermissionStatus::Granted.allows_execution());
    }
    #[test]
    fn permission_status_requires_approval_denies() {
        assert!(!PermissionStatus::RequiresApproval.allows_execution());
    }
    #[test]
    fn external_effect_unknown_is_effectful() {
        assert!(ExternalEffectKind::Unknown.is_effectful());
    }
    #[test]
    fn external_effect_policy_disabled_denies() {
        assert!(!ExternalEffectPolicy::disabled(ExternalEffectKind::ApiRead).allows_execution());
    }

    #[test]
    fn external_effect_policy_unknown_or_none_never_allows_execution() {
        assert!(
            !ExternalEffectPolicy::enabled_read_only(ExternalEffectKind::Unknown)
                .allows_execution()
        );
        assert!(
            !ExternalEffectPolicy::enabled_read_only(ExternalEffectKind::None).allows_execution()
        );
    }
    #[test]
    fn external_effect_policy_requires_approval_denies() {
        assert!(
            !ExternalEffectPolicy::requires_approval(ExternalEffectKind::ApiWrite)
                .allows_execution()
        );
    }
    #[test]
    fn dry_run_safe_is_safe() {
        assert!(DryRunSafety::Safe.is_safe());
    }
    #[test]
    fn dry_run_unsafe_write_is_unsafe() {
        assert!(!DryRunSafety::UnsafeWouldWrite.is_safe());
    }
    #[test]
    fn approval_granted_allows() {
        assert!(ApprovalRequirement::Granted.allows_execution());
    }
    #[test]
    fn redaction_strict_disallows_raw_sensitive_values() {
        assert!(!RedactionPolicy::strict().allows_raw_sensitive_values());
    }
    #[test]
    fn data_sensitivity_pii_requires_redaction() {
        assert!(DataSensitivity::Pii.requires_redaction());
    }
    #[test]
    fn sensitive_field_rejects_empty() {
        assert!(SensitiveField::new("", DataSensitivity::Pii).is_err());
    }
    #[test]
    fn agent_safety_dry_run_only_disallows_execution() {
        assert!(!AgentSafetyMode::AgentDryRunOnly.allows_execution());
    }
    #[test]
    fn agent_safety_with_approval_allows_external_effects() {
        assert!(AgentSafetyMode::AgentWithApproval.allows_external_effects());
    }
    #[test]
    fn security_plan_default_safe_disallows_external_effects() {
        assert!(!SecurityPlan::default_safe().allows_external_effects());
    }
    #[test]
    fn security_plan_unsupported_has_errors_and_no_fallback() {
        let p = SecurityPlan::unsupported("x", "y");
        assert!(p.has_errors());
        assert!(p.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
    #[test]
    fn security_plan_text_includes_fallback_disabled() {
        assert!(
            SecurityPlan::default_safe()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn audit_record_rejects_empty_subject() {
        assert!(AuditRecord::new(AuditActionKind::PlanCreated, "  ").is_err());
    }
    #[test]
    fn security_report_from_plan_no_effects_no_errors_default_safe() {
        let r = SecurityReport::from_plan(SecurityPlan::default_safe());
        assert!(!r.has_errors());
        assert!(!r.plan.allows_external_effects());
    }

    #[test]
    fn runtime_input_safety_report_blocks_malformed_inputs_without_fallback() {
        let report = RuntimeInputSafetyReport::malformed_without_panic(
            "vortex",
            "fixtures/malformed.vortex",
        );

        assert_eq!(
            report.schema_version,
            "shardloom.runtime_input_safety_report.v1"
        );
        assert!(report.deterministic_no_panic());
        assert!(report.no_fallback_invariant_holds());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::InvalidInput);
        assert!(!report.diagnostics[0].fallback.attempted);
    }

    #[test]
    fn runtime_input_safety_report_covers_utf8_size_and_depth_blockers() {
        let invalid_utf8 = RuntimeInputSafetyReport::invalid_utf8_without_panic("fixtures/bad.csv");
        assert_eq!(
            invalid_utf8.invalid_utf8_policy,
            "deterministic_invalid_input_diagnostic"
        );
        assert!(invalid_utf8.deterministic_no_panic());
        assert!(invalid_utf8.no_fallback_invariant_holds());

        let oversized = RuntimeInputSafetyReport::oversized_or_deeply_nested_blocker(
            "jsonl",
            "fixtures/deep.jsonl",
        );
        assert_eq!(
            oversized.max_depth_policy,
            "bounded_by_calling_surface_or_blocked"
        );
        assert!(
            oversized.diagnostics[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("oversized or deeply nested input")
        );
        assert!(oversized.no_fallback_invariant_holds());
    }

    #[test]
    fn workspace_path_safety_accepts_workspace_scoped_outputs() {
        let workspace = workspace_path_fixture_root("workspace");
        let report = WorkspacePathSafetyReport::evaluate(&workspace, "results/out.vortex");

        assert_eq!(
            report.schema_version,
            "shardloom.workspace_path_safety_report.v1"
        );
        assert!(report.accepted());
        assert!(report.within_workspace);
        assert!(!report.symlink_followed);
        assert_eq!(report.overwrite_policy, "explicit_only");
        assert!(report.no_fallback_invariant_holds());
    }

    #[test]
    fn workspace_path_safety_rejects_parent_traversal_and_external_outputs() {
        let workspace = workspace_path_fixture_root("workspace");
        let traversal = WorkspacePathSafetyReport::evaluate(&workspace, "../escape/out.vortex");
        assert!(!traversal.accepted());
        assert!(!traversal.within_workspace);
        assert!(traversal.path_traversal_checked);
        assert_eq!(traversal.diagnostics[0].code, DiagnosticCode::InvalidInput);
        assert!(traversal.no_fallback_invariant_holds());

        let external = WorkspacePathSafetyReport::evaluate(
            &workspace,
            workspace_path_fixture_root("other").join("out.vortex"),
        );
        assert!(!external.accepted());
        assert!(!external.within_workspace);
        assert!(external.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .reason
                .as_deref()
                .unwrap()
                .contains("outside the declared workspace")
        }));
        assert!(external.no_fallback_invariant_holds());
    }

    fn workspace_write_fixture_root(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "shardloom_workspace_write_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create workspace write fixture");
        root
    }

    #[test]
    fn workspace_safe_local_write_commits_with_staging_evidence() {
        let workspace = workspace_write_fixture_root("commit");
        let report = write_workspace_safe_bytes(
            &workspace,
            "results/out.jsonl",
            false,
            "test local output",
            b"{\"id\":1}\n",
        )
        .expect("workspace-safe write succeeds");

        let output_path = workspace.join("results/out.jsonl");
        assert_eq!(std::fs::read(&output_path).unwrap(), b"{\"id\":1}\n");
        assert_eq!(report.commit_status, "committed");
        assert_eq!(report.commit_mode, "atomic_rename_same_directory");
        assert_eq!(report.cleanup_status, "no_staging_artifacts_remaining");
        assert!(!report.staging_path.exists());
        assert!(report.path_safety_report.accepted());
        assert!(report.no_fallback_invariant_holds());
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_producer_write_streams_digest_and_staging_evidence() {
        let workspace = workspace_write_fixture_root("producer_commit");
        let (chunks, report) = write_workspace_safe_bytes_with_producer(
            &workspace,
            "results/out.bin",
            false,
            "test local producer output",
            |writer| {
                writer.write_all(b"alpha").unwrap();
                writer.write_all(b"-").unwrap();
                writer.write_all(b"beta").unwrap();
                Ok(3_u8)
            },
        )
        .expect("workspace-safe producer write succeeds");

        let output_path = workspace.join("results/out.bin");
        assert_eq!(chunks, 3);
        assert_eq!(std::fs::read(&output_path).unwrap(), b"alpha-beta");
        assert_eq!(report.bytes_written, 10);
        assert_eq!(report.output_digest, fnv64_digest_bytes(b"alpha-beta"));
        assert_eq!(report.commit_status, "committed");
        assert_eq!(report.commit_mode, "atomic_rename_same_directory");
        assert!(!report.staging_path.exists());
        assert!(report.path_safety_report.accepted());
        assert!(report.no_fallback_invariant_holds());
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_producer_write_cleans_staging_on_error() {
        let workspace = workspace_write_fixture_root("producer_error");
        let error = write_workspace_safe_bytes_with_producer(
            &workspace,
            "results/out.bin",
            false,
            "test local producer output",
            |writer| {
                writer.write_all(b"partial").unwrap();
                Err::<(), _>(ShardLoomError::InvalidOperation(
                    "producer failed intentionally; no fallback execution was attempted"
                        .to_string(),
                ))
            },
        )
        .expect_err("producer failure is returned");

        assert!(error.message().contains("producer failed intentionally"));
        assert!(!workspace.join("results/out.bin").exists());
        let results_dir = workspace.join("results");
        let remaining_entries = if results_dir.exists() {
            std::fs::read_dir(&results_dir).unwrap().count()
        } else {
            0
        };
        assert_eq!(remaining_entries, 0);
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_producer_validation_error_preserves_existing_target() {
        let workspace = workspace_write_fixture_root("producer_validation_error");
        let output_path = workspace.join("results/out.bin");
        std::fs::create_dir_all(output_path.parent().unwrap()).unwrap();
        std::fs::write(&output_path, b"original").unwrap();

        let error = write_workspace_safe_bytes_with_validated_producer(
            &workspace,
            "results/out.bin",
            true,
            "test validated local producer output",
            |writer| {
                writer.write_all(b"replacement").unwrap();
                Ok(7_u8)
            },
            |_| {
                Err(ShardLoomError::InvalidOperation(
                    "producer validation failed intentionally; no fallback execution was attempted"
                        .to_string(),
                ))
            },
        )
        .expect_err("validation failure is returned");

        assert!(
            error
                .message()
                .contains("producer validation failed intentionally")
        );
        assert_eq!(std::fs::read(&output_path).unwrap(), b"original");
        let remaining_entries = std::fs::read_dir(output_path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(remaining_entries, vec![output_path.clone()]);
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_write_blocks_traversal_before_writing() {
        let workspace = workspace_write_fixture_root("traversal");
        let error = write_workspace_safe_bytes(
            &workspace,
            "../shardloom_workspace_write_escape/out.jsonl",
            false,
            "test local output",
            b"x\n",
        )
        .expect_err("parent traversal is rejected");

        assert!(error.message().contains("parent-directory traversal"));
        assert!(
            !workspace
                .parent()
                .unwrap()
                .join("shardloom_workspace_write_escape/out.jsonl")
                .exists()
        );
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_write_requires_explicit_overwrite_and_replaces_safely() {
        let workspace = workspace_write_fixture_root("overwrite");
        let output_path = workspace.join("out.csv");
        std::fs::write(&output_path, b"old\n").unwrap();

        let blocked =
            write_workspace_safe_bytes(&workspace, "out.csv", false, "test local output", b"new\n")
                .expect_err("overwrite requires explicit permission");
        assert!(blocked.message().contains("overwrite is disabled"));
        assert_eq!(std::fs::read(&output_path).unwrap(), b"old\n");

        let report =
            write_workspace_safe_bytes(&workspace, "out.csv", true, "test local output", b"new\n")
                .expect("explicit overwrite succeeds");
        assert_eq!(std::fs::read(&output_path).unwrap(), b"new\n");
        assert!(report.overwrite_performed);
        assert_eq!(
            report.commit_mode,
            "staged_replace_with_backup_same_directory"
        );
        assert!(report.no_fallback_invariant_holds());
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_write_rejects_symlink_targets_when_supported() {
        let workspace = workspace_write_fixture_root("symlink");
        let real_target = workspace.join("real.txt");
        let symlink_target = workspace.join("link.txt");
        std::fs::write(&real_target, b"real").unwrap();

        #[cfg(unix)]
        let symlink_result = std::os::unix::fs::symlink(&real_target, &symlink_target);
        #[cfg(windows)]
        let symlink_result = std::os::windows::fs::symlink_file(&real_target, &symlink_target);
        #[cfg(not(any(unix, windows)))]
        let symlink_result: std::io::Result<()> = Err(std::io::Error::other("unsupported"));

        if symlink_result.is_ok() {
            let error = write_workspace_safe_bytes(
                &workspace,
                "link.txt",
                true,
                "test local output",
                b"new",
            )
            .expect_err("symlink output is rejected");
            assert!(error.message().contains("symlink"));
            assert_eq!(std::fs::read(&real_target).unwrap(), b"real");
        }
        let _ = std::fs::remove_file(&symlink_target);
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_write_allows_canonical_symlink_roots_when_supported() {
        let workspace = workspace_write_fixture_root("symlink_root");
        let real_root = workspace.join("real-root");
        let symlink_root = workspace.join("link-root");
        std::fs::create_dir_all(&real_root).unwrap();

        #[cfg(unix)]
        let symlink_result = std::os::unix::fs::symlink(&real_root, &symlink_root);
        #[cfg(windows)]
        let symlink_result = std::os::windows::fs::symlink_dir(&real_root, &symlink_root);
        #[cfg(not(any(unix, windows)))]
        let symlink_result: std::io::Result<()> = Err(std::io::Error::other("unsupported"));

        if symlink_result.is_ok() {
            let report = write_workspace_safe_bytes(
                &symlink_root,
                "out.jsonl",
                true,
                "test local output",
                b"x\n",
            )
            .expect("canonical symlink workspace root is accepted");
            assert_eq!(std::fs::read(real_root.join("out.jsonl")).unwrap(), b"x\n");
            assert_eq!(
                report.path_safety_report.symlink_policy,
                "canonical_workspace_root_symlink_allowed_output_symlinks_not_followed"
            );
            assert!(report.path_safety_report.accepted());
            assert!(report.no_fallback_invariant_holds());
        }
        let _ = std::fs::remove_file(&symlink_root);
        let _ = std::fs::remove_dir(&symlink_root);
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_safe_local_write_rejects_existing_symlink_parent_before_mkdir() {
        let workspace = workspace_write_fixture_root("symlink_parent");
        let real_parent = workspace.join("real-parent");
        let symlink_parent = workspace.join("link-parent");
        std::fs::create_dir_all(&real_parent).unwrap();

        #[cfg(unix)]
        let symlink_result = std::os::unix::fs::symlink(&real_parent, &symlink_parent);
        #[cfg(windows)]
        let symlink_result = std::os::windows::fs::symlink_dir(&real_parent, &symlink_parent);
        #[cfg(not(any(unix, windows)))]
        let symlink_result: std::io::Result<()> = Err(std::io::Error::other("unsupported"));

        if symlink_result.is_ok() {
            let error = write_workspace_safe_bytes(
                &workspace,
                "link-parent/nested/out.jsonl",
                true,
                "test local output",
                b"new",
            )
            .expect_err("symlink parent is rejected before directory creation");
            assert!(error.message().contains("symlink"));
            assert!(!real_parent.join("nested").exists());
        }
        let _ = std::fs::remove_file(&symlink_parent);
        let _ = std::fs::remove_dir(&symlink_parent);
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn evidence_artifact_safety_redacts_credential_like_values() {
        let report = EvidenceArtifactSafetyReport::inspect_text(
            "runtime-diagnostic",
            "Authorization: Bearer sk-live token=abc123 schema=orders",
            &RedactionPolicy::strict(),
        );

        assert_eq!(
            report.schema_version,
            "shardloom.evidence_artifact_safety_report.v1"
        );
        assert!(report.contains_credentials);
        assert!(report.contains_schema_names);
        assert!(!report.export_allowed);
        assert!(!report.agent_visible);
        assert!(report.redacted_preview.contains("<redacted>"));
        assert!(report.no_raw_credential_preview("sk-live"));
        assert!(report.no_raw_credential_preview("abc123"));
        assert!(report.no_fallback_invariant_holds());
    }

    #[test]
    fn security_governance_evidence_gate_covers_required_areas() {
        let report = plan_security_governance_evidence_gate();
        assert_eq!(report.evidence_area_count(), 8);
        assert_eq!(report.report_only_area_count(), 8);
        assert!(report.all_evidence_surfaces_present());
        assert!(report.all_required_evidence_areas_present());
        assert_eq!(report.missing_required_area_count(), 0);
        assert!(
            report
                .area_order()
                .contains(&SecurityGovernanceEvidenceArea::CredentialReference.as_str())
        );
        assert!(
            report
                .area_order()
                .contains(&SecurityGovernanceEvidenceArea::AgentPolicy.as_str())
        );
    }

    #[test]
    fn security_governance_evidence_gate_rejects_missing_required_areas() {
        let mut report = plan_security_governance_evidence_gate();
        report
            .entries
            .retain(|entry| entry.area != SecurityGovernanceEvidenceArea::AuditTrail);

        assert_eq!(report.missing_required_area_count(), 1);
        assert!(!report.all_required_evidence_areas_present());
        assert!(!report.all_evidence_surfaces_present());
        assert!(report.has_errors());
    }

    #[test]
    fn security_governance_evidence_gate_blocks_effects_and_claims_by_default() {
        let report = plan_security_governance_evidence_gate();
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(report.claims_blocked_by_default());
        assert_eq!(report.effectful_claim_allowed_count(), 0);
        assert!(report.effectful_features_default_denied);
        assert!(report.dry_run_required_without_policy);
        assert!(report.credential_references_only);
        assert!(!report.credentials_resolved);
        assert!(!report.secrets_loaded);
        assert!(!report.external_effects_executed);
        assert!(!report.destructive_operations_allowed);
        assert!(!report.data_egress_allowed);
        assert!(!report.agent_execute_write_cancel_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn security_governance_evidence_gate_requires_redaction_and_audit() {
        let mut report = plan_security_governance_evidence_gate();
        report.redaction_required = false;
        assert!(!report.side_effect_free());
        assert!(report.has_errors());

        let mut report = plan_security_governance_evidence_gate();
        report.audit_required = false;
        assert!(!report.side_effect_free());
        assert!(report.has_errors());
    }

    #[test]
    fn credential_policy_enforcement_gate_blocks_secret_runtime_by_default() {
        let report = plan_credential_policy_enforcement_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.credential_policy_enforcement_gate.v1"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(report.all_credential_runtime_blocked());
        assert!(
            report
                .row_order()
                .contains(&"credential_reference_inventory")
        );
        assert!(report.row_order().contains(&"secret_loading"));
        assert!(report.row_order().contains(&"runtime_permission_check"));
        assert!(report.credential_references_only);
        assert!(!report.credential_resolution_performed);
        assert!(!report.secret_loading_performed);
        assert!(!report.secret_value_materialized);
        assert!(!report.production_policy_runtime_supported);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(report.rows.iter().all(|row| !row.secret_loading_performed));
        assert!(
            report
                .rows
                .iter()
                .all(|row| !row.credential_resolution_performed)
        );
    }

    #[test]
    fn sandbox_governance_readiness_gate_blocks_runtime_by_default() {
        let report = plan_sandbox_governance_readiness_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.sandbox_governance_readiness_gate.v1"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(report.all_sandbox_runtime_blocked());
        assert!(report.row_order().contains(&"sandbox_profile_inventory"));
        assert!(report.row_order().contains(&"filesystem_permission"));
        assert!(report.row_order().contains(&"network_permission"));
        assert!(report.row_order().contains(&"process_execution"));
        assert!(report.deny_by_default);
        assert!(!report.sandbox_runtime_supported);
        assert!(!report.sandbox_process_spawned);
        assert!(!report.extension_code_executed);
        assert!(!report.udf_code_executed);
        assert!(!report.filesystem_access_allowed);
        assert!(!report.network_access_allowed);
        assert!(!report.environment_access_allowed);
        assert!(!report.secret_access_allowed);
        assert!(!report.process_execution_allowed);
        assert!(!report.resource_limits_enforced);
        assert!(!report.timeout_enforced);
        assert!(report.audit_required);
        assert!(!report.audit_log_runtime_supported);
        assert!(report.deterministic_unsupported_diagnostics);
        assert!(!report.production_governance_runtime_supported);
        assert!(!report.external_effect_executed);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(report.rows.iter().all(|row| !row.sandbox_enforced));
        assert!(report.rows.iter().all(|row| !row.external_effect_executed));
    }
}
