//! Native-first Plan IR and interoperability metadata skeleton.
//!
//! This module defines domain types only. It does not parse, serialize, import,
//! export, or execute plans.

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, EffectLevel, FidelityLevel,
    MaterializationRequirement, OutputTargetKind, Result, ShardLoomError,
};

/// Stable identifier for a native plan document.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanId(String);

impl PlanId {
    /// Creates a validated plan identifier.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the id is empty/whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSchemaVersion {
    pub name: String,
    pub version: u32,
}
impl PlanSchemaVersion {
    /// Creates a validated plan schema version.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] for empty names or non-positive versions.
    pub fn new(name: impl Into<String>, version: u32) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan schema name must not be empty".to_string(),
            ));
        }
        if version == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "plan schema version must be greater than zero".to_string(),
            ));
        }
        Ok(Self { name, version })
    }
    #[must_use]
    pub fn shardloom_v1() -> Self {
        Self {
            name: "shardloom.plan_ir".to_string(),
            version: 1,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}@v{}", self.name, self.version)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanLayer {
    UserIntent,
    Logical,
    OptimizedLogical,
    Physical,
    EncodedPhysical,
    Streaming,
    RuntimeTaskGraph,
    AdaptiveRuntime,
    ExecutedReport,
    Unsupported,
}
impl PlanLayer {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UserIntent => "user_intent",
            Self::Logical => "logical",
            Self::OptimizedLogical => "optimized_logical",
            Self::Physical => "physical",
            Self::EncodedPhysical => "encoded_physical",
            Self::Streaming => "streaming",
            Self::RuntimeTaskGraph => "runtime_task_graph",
            Self::AdaptiveRuntime => "adaptive_runtime",
            Self::ExecutedReport => "executed_report",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
    #[must_use]
    pub const fn is_executable_layer(&self) -> bool {
        matches!(
            self,
            Self::Physical
                | Self::EncodedPhysical
                | Self::Streaming
                | Self::RuntimeTaskGraph
                | Self::AdaptiveRuntime
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePlanNodeKind {
    Scan,
    Filter,
    Projection,
    Aggregate,
    Join,
    Sort,
    Limit,
    Udf,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    EmbeddingGeneration,
    VectorSearch,
    Translation,
    Write,
    Commit,
    Unsupported,
}
impl NativePlanNodeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::Sort => "sort",
            Self::Limit => "limit",
            Self::Udf => "udf",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::Translation => "translation",
            Self::Write => "write",
            Self::Commit => "commit",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        matches!(
            self,
            Self::ExternalRead
                | Self::ExternalWrite
                | Self::ModelCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanBoundaryKind {
    NativeVortexInput,
    NativeVortexOutput,
    CompatibilityOutput,
    Effect,
    Translation,
    Materialization,
    ZeroDecode,
    ZeroCopyInterop,
    Spill,
    Shuffle,
    Distributed,
    Unsupported,
}
impl PlanBoundaryKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexInput => "native_vortex_input",
            Self::NativeVortexOutput => "native_vortex_output",
            Self::CompatibilityOutput => "compatibility_output",
            Self::Effect => "effect",
            Self::Translation => "translation",
            Self::Materialization => "materialization",
            Self::ZeroDecode => "zero_decode",
            Self::ZeroCopyInterop => "zero_copy_boundary",
            Self::Spill => "spill",
            Self::Shuffle => "shuffle",
            Self::Distributed => "distributed",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_special_handling(&self) -> bool {
        matches!(
            self,
            Self::Effect
                | Self::Translation
                | Self::Materialization
                | Self::Spill
                | Self::Shuffle
                | Self::Distributed
                | Self::CompatibilityOutput
                | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCapabilityKind {
    VortexNativeInput,
    VortexNativeOutput,
    Statistics,
    ByteRanges,
    EncodedKernel,
    PartialDecode,
    Materialization,
    Streaming,
    SpillSupport,
    ObjectStoreAccess,
    ExternalCredentials,
    ExplicitEffectEnablement,
    CompatibilityOutput,
    NativeExecution,
    Unsupported,
}
impl PlanCapabilityKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::VortexNativeInput => "vortex_native_input",
            Self::VortexNativeOutput => "vortex_native_output",
            Self::Statistics => "statistics",
            Self::ByteRanges => "byte_ranges",
            Self::EncodedKernel => "encoded_kernel",
            Self::PartialDecode => "partial_decode",
            Self::Materialization => "materialization",
            Self::Streaming => "streaming",
            Self::SpillSupport => "spill_support",
            Self::ObjectStoreAccess => "object_store_access",
            Self::ExternalCredentials => "external_credentials",
            Self::ExplicitEffectEnablement => "explicit_effect_enablement",
            Self::CompatibilityOutput => "compatibility_output",
            Self::NativeExecution => "native_execution",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCapabilityRequirement {
    pub kind: PlanCapabilityKind,
    pub required: bool,
    pub reason: String,
}
impl PlanCapabilityRequirement {
    #[must_use]
    pub fn required(kind: PlanCapabilityKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            required: true,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn optional(kind: PlanCapabilityKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            required: false,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "capability={} required={} reason={}",
            self.kind.as_str(),
            self.required,
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBoundary {
    pub effect_level: EffectLevel,
    pub requires_explicit_enablement: bool,
    pub requires_credentials: bool,
    pub dry_run_safe: bool,
    pub reason: String,
}
impl EffectBoundary {
    #[must_use]
    pub fn new(effect_level: EffectLevel, reason: impl Into<String>) -> Self {
        Self {
            effect_level,
            requires_explicit_enablement: !matches!(
                effect_level,
                EffectLevel::PureDeterministic | EffectLevel::PureNondeterministic
            ),
            requires_credentials: false,
            dry_run_safe: true,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn requires_enablement(mut self, value: bool) -> Self {
        self.requires_explicit_enablement = value;
        self
    }
    #[must_use]
    pub fn requires_credentials(mut self, value: bool) -> Self {
        self.requires_credentials = value;
        self
    }
    #[must_use]
    pub fn dry_run_safe(mut self, value: bool) -> Self {
        self.dry_run_safe = value;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "effect_level={} requires_explicit_enablement={} requires_credentials={} dry_run_safe={} reason={}",
            self.effect_level.as_str(),
            self.requires_explicit_enablement,
            self.requires_credentials,
            self.dry_run_safe,
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslationBoundary {
    pub target_kind: OutputTargetKind,
    pub fidelity: FidelityLevel,
    pub materialization: MaterializationRequirement,
    pub metadata_loss_expected: bool,
}
impl TranslationBoundary {
    #[must_use]
    pub fn from_target_kind(target_kind: OutputTargetKind) -> Self {
        let fidelity = target_kind.default_fidelity();
        let materialization = target_kind.default_materialization_requirement();
        let metadata_loss_expected = !matches!(target_kind, OutputTargetKind::Vortex);
        Self {
            target_kind,
            fidelity,
            materialization,
            metadata_loss_expected,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "translation target={} fidelity={} materialization={} metadata_loss_expected={}",
            self.target_kind.as_str(),
            self.fidelity.as_str(),
            self.materialization.to_human_text(),
            self.metadata_loss_expected
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NativePlanNode {
    pub id: crate::PlanNodeId,
    pub layer: PlanLayer,
    pub kind: NativePlanNodeKind,
    pub label: String,
    pub capabilities: Vec<PlanCapabilityRequirement>,
    pub boundaries: Vec<PlanBoundaryKind>,
    pub diagnostics: Vec<Diagnostic>,
}
impl NativePlanNode {
    #[must_use]
    pub fn new(
        id: crate::PlanNodeId,
        layer: PlanLayer,
        kind: NativePlanNodeKind,
        label: impl Into<String>,
    ) -> Self {
        Self {
            id,
            layer,
            kind,
            label: label.into(),
            capabilities: Vec::new(),
            boundaries: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_capability(&mut self, capability: PlanCapabilityRequirement) {
        self.capabilities.push(capability);
    }
    pub fn add_boundary(&mut self, boundary: PlanBoundaryKind) {
        self.boundaries.push(boundary);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn is_effectful(&self) -> bool {
        self.kind.is_effectful() || self.boundaries.contains(&PlanBoundaryKind::Effect)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "node[id={} layer={} kind={} label={} effectful={} capabilities={} boundaries={} diagnostics={}]",
            self.id.as_str(),
            self.layer.as_str(),
            self.kind.as_str(),
            self.label,
            self.is_effectful(),
            self.capabilities.len(),
            self.boundaries.len(),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanValidationStatus {
    Valid,
    Warning,
    Invalid,
    Unsupported,
    NotValidated,
}
impl PlanValidationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Warning => "warning",
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
            Self::NotValidated => "not_validated",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Invalid | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanValidationReport {
    pub status: PlanValidationStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub checked_capabilities: Vec<PlanCapabilityRequirement>,
    pub fallback_execution_allowed: bool,
}
impl PlanValidationReport {
    #[must_use]
    pub fn not_validated() -> Self {
        Self {
            status: PlanValidationStatus::NotValidated,
            diagnostics: Vec::new(),
            checked_capabilities: Vec::new(),
            fallback_execution_allowed: false,
        }
    }
    #[must_use]
    pub fn valid() -> Self {
        Self {
            status: PlanValidationStatus::Valid,
            diagnostics: Vec::new(),
            checked_capabilities: Vec::new(),
            fallback_execution_allowed: false,
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut s = Self {
            status: PlanValidationStatus::Unsupported,
            diagnostics: Vec::new(),
            checked_capabilities: Vec::new(),
            fallback_execution_allowed: false,
        };
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            format!("Plan validation unsupported: {}", reason.into()),
            Some("Use a supported native ShardLoom plan path.".to_string()),
        ));
        s
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn add_checked_capability(&mut self, capability: PlanCapabilityRequirement) {
        self.checked_capabilities.push(capability);
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
    pub fn summary(&self) -> String {
        format!(
            "validation_status={} diagnostics={} checked_capabilities={} fallback_execution_allowed={}",
            self.status.as_str(),
            self.diagnostics.len(),
            self.checked_capabilities.len(),
            self.fallback_execution_allowed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanInteropFormat {
    ShardLoomNative,
    AgentPlanSpec,
    SubstraitLike,
    JsonLike,
    Unknown,
}
impl PlanInteropFormat {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ShardLoomNative => "native",
            Self::AgentPlanSpec => "agent",
            Self::SubstraitLike => "substrait-like",
            Self::JsonLike => "json-like",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_external(&self) -> bool {
        !matches!(self, Self::ShardLoomNative)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanImportStatus {
    Planned,
    RequiresValidation,
    Unsupported,
    NotImplemented,
}
impl PlanImportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RequiresValidation => "requires_validation",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn requires_validation(&self) -> bool {
        matches!(self, Self::Planned | Self::RequiresValidation)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanImportRequest {
    pub format: PlanInteropFormat,
    pub schema_version: Option<PlanSchemaVersion>,
    pub source_label: String,
    pub status: PlanImportStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl PlanImportRequest {
    /// Creates an import request skeleton.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `source_label` is empty/whitespace.
    pub fn new(format: PlanInteropFormat, source_label: impl Into<String>) -> Result<Self> {
        let source_label = source_label.into();
        if source_label.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan import source_label must not be empty".to_string(),
            ));
        }
        Ok(Self {
            format,
            schema_version: None,
            source_label,
            status: PlanImportStatus::Planned,
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn with_schema_version(mut self, schema_version: PlanSchemaVersion) -> Self {
        self.schema_version = Some(schema_version);
        self.status = PlanImportStatus::RequiresValidation;
        self
    }
    /// Creates a not-implemented import request with deterministic no-fallback diagnostics.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `source_label` is empty/whitespace.
    pub fn not_implemented(
        format: PlanInteropFormat,
        source_label: impl Into<String>,
    ) -> Result<Self> {
        let mut s = Self::new(format, source_label)?;
        s.status = PlanImportStatus::NotImplemented;
        s.add_diagnostic(Diagnostic::not_implemented(
            "plan_import",
            "Plan import is metadata-only skeleton and not implemented for real import.",
            "Imported plans must pass ShardLoom-native validation once supported.",
        ));
        Ok(s)
    }
    /// Creates an unsupported import request with deterministic no-fallback diagnostics.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when `source_label` is empty/whitespace.
    pub fn unsupported(
        format: PlanInteropFormat,
        source_label: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let mut s = Self::new(format, source_label)?;
        s.status = PlanImportStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "plan_import",
            format!("Unsupported plan import: {}", reason.into()),
            Some("Use native plan authoring or a supported import format.".to_string()),
        ));
        Ok(s)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        matches!(
            self.status,
            PlanImportStatus::Unsupported | PlanImportStatus::NotImplemented
        ) || self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "plan_import format={} source_label={} status={} validation_required={} imported plans must pass ShardLoom-native validation fallback_execution_allowed=false",
            self.format.as_str(),
            self.source_label,
            self.status.as_str(),
            self.status.requires_validation()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanExportStatus {
    Planned,
    Unsupported,
    NotImplemented,
}
impl PlanExportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported | Self::NotImplemented)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanExportRequest {
    pub format: PlanInteropFormat,
    pub include_diagnostics: bool,
    pub include_estimates: bool,
    pub include_secrets: bool,
    pub status: PlanExportStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl PlanExportRequest {
    #[must_use]
    pub fn new(format: PlanInteropFormat) -> Self {
        Self {
            format,
            include_diagnostics: false,
            include_estimates: false,
            include_secrets: false,
            status: PlanExportStatus::Planned,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn include_diagnostics(mut self, value: bool) -> Self {
        self.include_diagnostics = value;
        self
    }
    #[must_use]
    pub fn include_estimates(mut self, value: bool) -> Self {
        self.include_estimates = value;
        self
    }
    #[must_use]
    pub fn include_secrets(mut self, value: bool) -> Self {
        self.include_secrets = value;
        if value {
            self.add_diagnostic(Diagnostic::metadata_loss_warning(
                "plan_export_secrets",
                "secret export is unsafe and not recommended",
                "Disable include_secrets for safe export metadata.",
            ));
        }
        self
    }
    #[must_use]
    pub fn not_implemented(format: PlanInteropFormat) -> Self {
        let mut s = Self::new(format);
        s.status = PlanExportStatus::NotImplemented;
        s.add_diagnostic(Diagnostic::not_implemented(
            "plan_export",
            "Plan export is metadata-only skeleton and real serialization is not implemented.",
            "Use native explain/report flows until export is implemented.",
        ));
        s
    }
    #[must_use]
    pub fn unsupported(format: PlanInteropFormat, reason: impl Into<String>) -> Self {
        let mut s = Self::new(format);
        s.status = PlanExportStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "plan_export",
            format!("Unsupported plan export: {}", reason.into()),
            Some("Use a supported export format or native output.".to_string()),
        ));
        s
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
    pub fn summary(&self) -> String {
        format!(
            "plan_export format={} status={} include_diagnostics={} include_estimates={} include_secrets={} fallback_execution_allowed=false",
            self.format.as_str(),
            self.status.as_str(),
            self.include_diagnostics,
            self.include_estimates,
            self.include_secrets
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NativePlanDocument {
    pub id: PlanId,
    pub schema_version: PlanSchemaVersion,
    pub layer: PlanLayer,
    pub nodes: Vec<NativePlanNode>,
    pub validation: PlanValidationReport,
    pub diagnostics: Vec<Diagnostic>,
}
impl NativePlanDocument {
    #[must_use]
    pub fn new(id: PlanId, layer: PlanLayer) -> Self {
        Self {
            id,
            schema_version: PlanSchemaVersion::shardloom_v1(),
            layer,
            nodes: Vec::new(),
            validation: PlanValidationReport::not_validated(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn empty(id: PlanId) -> Self {
        Self::new(id, PlanLayer::Logical)
    }
    pub fn add_node(&mut self, node: NativePlanNode) {
        self.nodes.push(node);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn validate_skeleton(&mut self) {
        self.validation.fallback_execution_allowed = false;
        self.validation.diagnostics.clear();
        self.validation.status = if self.nodes.is_empty() {
            self.validation.add_diagnostic(Diagnostic::new(
                DiagnosticCode::NotImplemented,
                DiagnosticSeverity::Warning,
                shardloom_core::DiagnosticCategory::Planning,
                "Plan document has zero nodes; skeleton is incomplete.",
                Some("plan_ir".to_string()),
                Some("Add at least one native node.".to_string()),
                Some("Construct a plan skeleton with nodes before execution planning.".to_string()),
                shardloom_core::FallbackStatus::disabled_by_policy(),
            ));
            PlanValidationStatus::Warning
        } else if self.has_errors() {
            PlanValidationStatus::Invalid
        } else {
            PlanValidationStatus::Valid
        };
    }
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self.nodes.iter().any(NativePlanNode::has_errors)
            || self.validation.has_errors()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "plan_id={} schema={} layer={} nodes={} validation={} fallback_execution_allowed=false",
            self.id.as_str(),
            self.schema_version.summary(),
            self.layer.as_str(),
            self.nodes.len(),
            self.validation.status.as_str()
        )
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{}\nvalidation: {}\nfallback execution: disabled",
            self.summary(),
            self.validation.summary()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlanNodeId;
    use shardloom_core::{DiagnosticCategory, FallbackStatus};
    #[test]
    fn plan_id_rejects_empty_ids() {
        assert!(PlanId::new("   ").is_err());
    }
    #[test]
    fn schema_rejects_zero() {
        assert!(PlanSchemaVersion::new("x", 0).is_err());
    }
    #[test]
    fn schema_v1_works() {
        let s = PlanSchemaVersion::shardloom_v1();
        assert_eq!(s.version, 1);
    }
    #[test]
    fn layer_physical_exec() {
        assert!(PlanLayer::Physical.is_executable_layer());
    }
    #[test]
    fn layer_report_not_exec() {
        assert!(!PlanLayer::ExecutedReport.is_executable_layer());
    }
    #[test]
    fn node_kind_external_read_effectful() {
        assert!(NativePlanNodeKind::ExternalRead.is_effectful());
    }
    #[test]
    fn node_kind_scan_not_effectful() {
        assert!(!NativePlanNodeKind::Scan.is_effectful());
    }
    #[test]
    fn boundary_effect_special() {
        assert!(PlanBoundaryKind::Effect.requires_special_handling());
    }
    #[test]
    fn boundary_native_input_not_special() {
        assert!(!PlanBoundaryKind::NativeVortexInput.requires_special_handling());
    }
    #[test]
    fn required_capability_sets_required() {
        assert!(PlanCapabilityRequirement::required(PlanCapabilityKind::Streaming, "x").required);
    }
    #[test]
    fn effect_boundary_defaults_enablement() {
        assert!(EffectBoundary::new(EffectLevel::ExternalRead, "x").requires_explicit_enablement);
    }
    #[test]
    fn translation_vortex_full_fidelity() {
        let t = TranslationBoundary::from_target_kind(OutputTargetKind::Vortex);
        assert_eq!(t.fidelity, FidelityLevel::NativeFullFidelity);
        assert!(!t.metadata_loss_expected);
    }
    #[test]
    fn translation_parquet_metadata_loss() {
        let t = TranslationBoundary::from_target_kind(OutputTargetKind::Parquet);
        assert!(t.metadata_loss_expected);
    }
    #[test]
    fn node_has_errors_detected() {
        let mut n = NativePlanNode::new(
            PlanNodeId::new("n1").expect("id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan",
        );
        n.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::Planning,
            "x",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(n.has_errors());
    }
    #[test]
    fn node_effectful_from_kind() {
        let n = NativePlanNode::new(
            PlanNodeId::new("n1").expect("id"),
            PlanLayer::Logical,
            NativePlanNodeKind::ExternalRead,
            "read",
        );
        assert!(n.is_effectful());
    }
    #[test]
    fn validation_unsupported_has_errors_and_no_fallback() {
        let r = PlanValidationReport::unsupported("f", "r");
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn interop_substrait_like_is_external() {
        assert!(PlanInteropFormat::SubstraitLike.is_external());
    }
    #[test]
    fn import_rejects_empty_source_label() {
        assert!(PlanImportRequest::new(PlanInteropFormat::Unknown, " ").is_err());
    }
    #[test]
    fn import_not_implemented_has_errors() {
        assert!(
            PlanImportRequest::not_implemented(PlanInteropFormat::JsonLike, "src")
                .expect("ok")
                .has_errors()
        );
    }
    #[test]
    fn export_defaults_include_secrets_false() {
        assert!(!PlanExportRequest::new(PlanInteropFormat::Unknown).include_secrets);
    }
    #[test]
    fn export_include_secrets_warns() {
        let e = PlanExportRequest::new(PlanInteropFormat::Unknown).include_secrets(true);
        assert!(
            e.diagnostics
                .iter()
                .any(|d| d.severity == DiagnosticSeverity::Warning)
        );
    }
    #[test]
    fn empty_doc_zero_nodes() {
        let d = NativePlanDocument::empty(PlanId::new("p").expect("id"));
        assert_eq!(d.node_count(), 0);
    }
    #[test]
    fn empty_doc_validate_warning_not_error() {
        let mut d = NativePlanDocument::empty(PlanId::new("p").expect("id"));
        d.validate_skeleton();
        assert_eq!(d.validation.status, PlanValidationStatus::Warning);
        assert!(!d.validation.has_errors());
    }
    #[test]
    fn human_text_mentions_fallback_disabled() {
        let mut d = NativePlanDocument::empty(PlanId::new("p").expect("id"));
        d.validate_skeleton();
        assert!(d.to_human_text().contains("fallback execution: disabled"));
    }
}
