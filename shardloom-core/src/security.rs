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

#[cfg(test)]
mod tests {
    use super::*;
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
}
