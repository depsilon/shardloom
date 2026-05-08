//! Native-first Plan IR and interoperability metadata skeleton.
//!
//! This module defines native plan domain types and side-effect-free native
//! serialization helpers. It does not execute plans, touch files, or delegate to
//! external engines.

use std::collections::BTreeSet;

use shardloom_core::{
    CapabilityCertificationReport, CapabilityCertificationStatus, Diagnostic, DiagnosticCode,
    DiagnosticSeverity, EffectLevel, FidelityLevel, MaterializationRequirement,
    OperatorCertificationStatus, OutputTargetKind, Result, ShardLoomError,
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
                | Self::Write
                | Self::Commit
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
pub enum PlanPortabilityDirection {
    NativeReview,
    ImportValidation,
    ExportValidation,
}
impl PlanPortabilityDirection {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeReview => "native_review",
            Self::ImportValidation => "import_validation",
            Self::ExportValidation => "export_validation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanPortabilityStatus {
    NativeSkeleton,
    ValidationOnly,
    Imported,
    Serialized,
    NotImplemented,
    Unsupported,
}
impl PlanPortabilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeSkeleton => "native_skeleton",
            Self::ValidationOnly => "validation_only",
            Self::Imported => "imported",
            Self::Serialized => "serialized",
            Self::NotImplemented => "not_implemented",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::NotImplemented | Self::Unsupported)
    }
}

/// Report-only CG-12 contract for native-first plan portability evidence.
///
/// The report records whether native plan constructs are native-only,
/// Substrait-like representable, lossy, unsupported, or residual. It is
/// validation metadata only: native serialization is in-memory and
/// side-effect-free, and the report never executes plans, probes storage, writes
/// output, or delegates to an external engine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct PlanPortabilityReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub direction: PlanPortabilityDirection,
    pub status: PlanPortabilityStatus,
    pub interop_format: PlanInteropFormat,
    pub native_plan_schema_version: PlanSchemaVersion,
    pub native_first: bool,
    pub validation_only: bool,
    pub validation_required: bool,
    pub capability_check_required: bool,
    pub supported_constructs: Vec<String>,
    pub native_only_nodes: Vec<String>,
    pub substrait_like_representable_nodes: Vec<String>,
    pub lossy_nodes: Vec<String>,
    pub unsupported_nodes: Vec<String>,
    pub residual_unsupported_constructs: Vec<String>,
    pub metadata_loss_boundaries: Vec<String>,
    pub encoded_semantics_loss: bool,
    pub redaction_required: bool,
    pub parser_executed: bool,
    pub import_export_serialization_performed: bool,
    pub runtime_execution: bool,
    pub external_engine_execution: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub read_io: bool,
    pub write_io: bool,
    pub side_effect_free: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl PlanPortabilityReport {
    #[must_use]
    pub fn native_skeleton(document: &NativePlanDocument) -> Self {
        let mut report = Self::base(
            format!("portability-native-{}", document.id.as_str()),
            PlanPortabilityDirection::NativeReview,
            PlanPortabilityStatus::NativeSkeleton,
            PlanInteropFormat::ShardLoomNative,
            document.schema_version.clone(),
        );
        report.supported_constructs = vec![
            "native_plan_document".to_string(),
            "native_plan_schema_version".to_string(),
            "validation_report".to_string(),
            "diagnostics".to_string(),
            "fallback_status".to_string(),
        ];
        report.validation_required = true;
        report.capability_check_required = true;
        for node in &document.nodes {
            report.record_node(node);
        }
        report
            .diagnostics
            .extend(document.validation.diagnostics.clone());
        report.diagnostics.extend(document.diagnostics.clone());
        report
    }

    #[must_use]
    pub fn for_import_request(request: &PlanImportRequest) -> Self {
        let mut report = Self::base(
            format!(
                "portability-import-{}-{}",
                request.format.as_str(),
                request.source_label
            ),
            PlanPortabilityDirection::ImportValidation,
            match request.status {
                PlanImportStatus::Imported => PlanPortabilityStatus::Imported,
                PlanImportStatus::Unsupported => PlanPortabilityStatus::Unsupported,
                PlanImportStatus::NotImplemented => PlanPortabilityStatus::NotImplemented,
                PlanImportStatus::Planned | PlanImportStatus::RequiresValidation => {
                    PlanPortabilityStatus::ValidationOnly
                }
            },
            request.format,
            request
                .schema_version
                .clone()
                .unwrap_or_else(PlanSchemaVersion::shardloom_v1),
        );
        report.supported_constructs = vec![
            "format_declaration".to_string(),
            "source_label".to_string(),
            "native_capability_check_required".to_string(),
            "diagnostics".to_string(),
        ];
        if request.format == PlanInteropFormat::ShardLoomNative {
            report
                .supported_constructs
                .insert(2, "native_plan_serialization".to_string());
        }
        report.validation_required = true;
        report.capability_check_required = true;
        report.import_export_serialization_performed =
            request.serialization_performed || request.imported_document.is_some();
        if let Some(document) = &request.imported_document {
            report.native_plan_schema_version = document.schema_version.clone();
            for node in &document.nodes {
                report.record_node(node);
            }
            report
                .diagnostics
                .extend(document.validation.diagnostics.clone());
            report.diagnostics.extend(document.diagnostics.clone());
        }
        if matches!(
            request.status,
            PlanImportStatus::Unsupported | PlanImportStatus::NotImplemented
        ) {
            report
                .unsupported_nodes
                .push("real_plan_import".to_string());
            report
                .residual_unsupported_constructs
                .push("plan_payload_parsing".to_string());
            report
                .residual_unsupported_constructs
                .push("native_lowering".to_string());
        }
        report.diagnostics.extend(request.diagnostics.clone());
        report
    }

    #[must_use]
    pub fn for_export_request(request: &PlanExportRequest) -> Self {
        let mut report = Self::base(
            format!("portability-export-{}", request.format.as_str()),
            PlanPortabilityDirection::ExportValidation,
            match request.status {
                PlanExportStatus::Serialized => PlanPortabilityStatus::Serialized,
                PlanExportStatus::Unsupported => PlanPortabilityStatus::Unsupported,
                PlanExportStatus::NotImplemented => PlanPortabilityStatus::NotImplemented,
                PlanExportStatus::Planned => PlanPortabilityStatus::ValidationOnly,
            },
            request.format,
            PlanSchemaVersion::shardloom_v1(),
        );
        report.supported_constructs = vec![
            "format_declaration".to_string(),
            "native_plan_schema_version".to_string(),
            "diagnostics".to_string(),
            "redaction_policy_required".to_string(),
        ];
        if request.format == PlanInteropFormat::ShardLoomNative {
            report
                .supported_constructs
                .insert(2, "native_plan_serialization".to_string());
        }
        report.validation_required = true;
        report.capability_check_required = true;
        report.redaction_required = true;
        report.import_export_serialization_performed = request.serialized_document.is_some();
        if matches!(
            request.status,
            PlanExportStatus::Unsupported | PlanExportStatus::NotImplemented
        ) {
            report
                .unsupported_nodes
                .push("real_plan_export".to_string());
            report
                .residual_unsupported_constructs
                .push("interop_serialization".to_string());
        }
        if request.include_secrets {
            report
                .metadata_loss_boundaries
                .push("secret_redaction_boundary".to_string());
            report
                .lossy_nodes
                .push("exported_secret_fields".to_string());
        }
        report.diagnostics.extend(request.diagnostics.clone());
        report
    }

    fn base(
        report_id: String,
        direction: PlanPortabilityDirection,
        status: PlanPortabilityStatus,
        interop_format: PlanInteropFormat,
        native_plan_schema_version: PlanSchemaVersion,
    ) -> Self {
        Self {
            schema_version: "shardloom.plan_portability.v1",
            report_id,
            direction,
            status,
            interop_format,
            native_plan_schema_version,
            native_first: true,
            validation_only: true,
            validation_required: false,
            capability_check_required: false,
            supported_constructs: Vec::new(),
            native_only_nodes: Vec::new(),
            substrait_like_representable_nodes: Vec::new(),
            lossy_nodes: Vec::new(),
            unsupported_nodes: Vec::new(),
            residual_unsupported_constructs: Vec::new(),
            metadata_loss_boundaries: Vec::new(),
            encoded_semantics_loss: false,
            redaction_required: false,
            parser_executed: false,
            import_export_serialization_performed: false,
            runtime_execution: false,
            external_engine_execution: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            read_io: false,
            write_io: false,
            side_effect_free: true,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    fn record_node(&mut self, node: &NativePlanNode) {
        let node_label = format!("{}:{}", node.id.as_str(), node.kind.as_str());
        match node.kind {
            NativePlanNodeKind::Scan
            | NativePlanNodeKind::Filter
            | NativePlanNodeKind::Projection
            | NativePlanNodeKind::Aggregate
            | NativePlanNodeKind::Join
            | NativePlanNodeKind::Sort
            | NativePlanNodeKind::Limit => self.substrait_like_representable_nodes.push(node_label),
            NativePlanNodeKind::Unsupported => self.unsupported_nodes.push(node_label),
            NativePlanNodeKind::Udf
            | NativePlanNodeKind::ExternalRead
            | NativePlanNodeKind::ExternalWrite
            | NativePlanNodeKind::ModelCall
            | NativePlanNodeKind::EmbeddingGeneration
            | NativePlanNodeKind::VectorSearch
            | NativePlanNodeKind::Translation
            | NativePlanNodeKind::Write
            | NativePlanNodeKind::Commit => self.native_only_nodes.push(node_label),
        }

        for boundary in &node.boundaries {
            match boundary {
                PlanBoundaryKind::CompatibilityOutput | PlanBoundaryKind::Translation => self
                    .metadata_loss_boundaries
                    .push(format!("{}:{}", node.id.as_str(), boundary.as_str())),
                PlanBoundaryKind::Materialization | PlanBoundaryKind::ZeroCopyInterop => self
                    .lossy_nodes
                    .push(format!("{}:{}", node.id.as_str(), boundary.as_str())),
                PlanBoundaryKind::Unsupported => self.unsupported_nodes.push(format!(
                    "{}:{}",
                    node.id.as_str(),
                    boundary.as_str()
                )),
                PlanBoundaryKind::NativeVortexInput
                | PlanBoundaryKind::NativeVortexOutput
                | PlanBoundaryKind::Effect
                | PlanBoundaryKind::ZeroDecode
                | PlanBoundaryKind::Spill
                | PlanBoundaryKind::Shuffle
                | PlanBoundaryKind::Distributed => {}
            }
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.parser_executed
            || self.runtime_execution
            || self.external_engine_execution
            || self.filesystem_probe
            || self.network_probe
            || self.catalog_probe
            || self.adapter_probe
            || self.read_io
            || self.write_io
            || !self.side_effect_free
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "plan portability report\nschema_version: {}\nreport_id: {}\ndirection: {}\nstatus: {}\ninterop_format: {}\nnative_plan_schema_version: {}\nnative_first: {}\nvalidation_only: {}\nvalidation_required: {}\ncapability_check_required: {}\nsupported_constructs: {}\nnative_only_nodes: {}\nsubstrait_like_representable_nodes: {}\nlossy_nodes: {}\nunsupported_nodes: {}\nresidual_unsupported_constructs: {}\nmetadata_loss_boundaries: {}\nruntime execution: disabled\nexternal engine execution: disabled\nimport/export serialization performed: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.direction.as_str(),
            self.status.as_str(),
            self.interop_format.as_str(),
            self.native_plan_schema_version.summary(),
            self.native_first,
            self.validation_only,
            self.validation_required,
            self.capability_check_required,
            format_list(&self.supported_constructs),
            format_list(&self.native_only_nodes),
            format_list(&self.substrait_like_representable_nodes),
            format_list(&self.lossy_nodes),
            format_list(&self.unsupported_nodes),
            format_list(&self.residual_unsupported_constructs),
            format_list(&self.metadata_loss_boundaries),
            self.import_export_serialization_performed,
        )
    }
}

fn format_list(values: &[String]) -> String {
    if values.is_empty() {
        "<none>".to_string()
    } else {
        values.join(", ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanImportStatus {
    Planned,
    RequiresValidation,
    Imported,
    Unsupported,
    NotImplemented,
}
impl PlanImportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RequiresValidation => "requires_validation",
            Self::Imported => "imported",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn requires_validation(&self) -> bool {
        matches!(
            self,
            Self::Planned
                | Self::RequiresValidation
                | Self::Imported
                | Self::Unsupported
                | Self::NotImplemented
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanImportRequest {
    pub format: PlanInteropFormat,
    pub schema_version: Option<PlanSchemaVersion>,
    pub source_label: String,
    pub status: PlanImportStatus,
    pub imported_document: Option<NativePlanDocument>,
    pub serialization_performed: bool,
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
            imported_document: None,
            serialization_performed: false,
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn with_schema_version(mut self, schema_version: PlanSchemaVersion) -> Self {
        self.schema_version = Some(schema_version);
        self.status = PlanImportStatus::RequiresValidation;
        self
    }
    /// Imports a ShardLoom-native serialized plan payload without filesystem IO.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the payload is empty or
    /// not a valid ShardLoom-native plan serialization.
    pub fn from_native_serialized(payload: impl Into<String>) -> Result<Self> {
        let payload = payload.into();
        let document = NativePlanDocument::from_native_serialized(&payload)?;
        let mut s = Self::new(PlanInteropFormat::ShardLoomNative, document.id.as_str())?;
        s.schema_version = Some(document.schema_version.clone());
        s.status = PlanImportStatus::Imported;
        s.serialization_performed = true;
        s.imported_document = Some(document);
        Ok(s)
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
pub enum ImportedPlanCapabilityGateStatus {
    NoImportedPlan,
    BlockedInvalidPlan,
    BlockedEffectBoundary,
    BlockedMissingCapabilityEvidence,
    CapabilityChecked,
}

impl ImportedPlanCapabilityGateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoImportedPlan => "no_imported_plan",
            Self::BlockedInvalidPlan => "blocked_invalid_plan",
            Self::BlockedEffectBoundary => "blocked_effect_boundary",
            Self::BlockedMissingCapabilityEvidence => "blocked_missing_capability_evidence",
            Self::CapabilityChecked => "capability_checked",
        }
    }

    #[must_use]
    pub const fn allows_execution(&self) -> bool {
        matches!(self, Self::CapabilityChecked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ImportedPlanCapabilityGateReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: ImportedPlanCapabilityGateStatus,
    pub imported_plan_id: Option<String>,
    pub imported_plan_node_count: usize,
    pub capability_checked: bool,
    pub required_capability_surfaces: Vec<String>,
    pub certified_capability_surfaces: Vec<String>,
    pub missing_certification_surfaces: Vec<String>,
    pub unsupported_node_count: usize,
    pub effect_boundary_count: usize,
    pub execution_allowed: bool,
    pub runtime_execution: bool,
    pub parser_executed: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub external_engine_execution: bool,
    pub read_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl ImportedPlanCapabilityGateReport {
    #[must_use]
    pub fn for_import_request(
        request: &PlanImportRequest,
        certification: &CapabilityCertificationReport,
    ) -> Self {
        let mut report = Self::base(request);
        let Some(document) = &request.imported_document else {
            report.status = ImportedPlanCapabilityGateStatus::NoImportedPlan;
            report
                .missing_certification_surfaces
                .push("native_plan_document".to_string());
            return report;
        };

        report.imported_plan_id = Some(document.id.as_str().to_string());
        report.imported_plan_node_count = document.node_count();
        report.unsupported_node_count = document
            .nodes
            .iter()
            .filter(|node| node.kind == NativePlanNodeKind::Unsupported)
            .count();
        report.effect_boundary_count = document
            .nodes
            .iter()
            .filter(|node| {
                node.kind.is_effectful() || node.boundaries.contains(&PlanBoundaryKind::Effect)
            })
            .count();
        report.required_capability_surfaces = imported_plan_required_surfaces(document);
        report.certified_capability_surfaces = report
            .required_capability_surfaces
            .iter()
            .filter(|surface| imported_plan_surface_certified(certification, surface))
            .cloned()
            .collect();
        report.missing_certification_surfaces = report
            .required_capability_surfaces
            .iter()
            .filter(|surface| !imported_plan_surface_certified(certification, surface))
            .cloned()
            .collect();

        report.capability_checked = true;
        report.status =
            if request.has_errors() || document.has_errors() || report.unsupported_node_count > 0 {
                ImportedPlanCapabilityGateStatus::BlockedInvalidPlan
            } else if report.effect_boundary_count > 0 {
                ImportedPlanCapabilityGateStatus::BlockedEffectBoundary
            } else if report.missing_certification_surfaces.is_empty() {
                ImportedPlanCapabilityGateStatus::CapabilityChecked
            } else {
                ImportedPlanCapabilityGateStatus::BlockedMissingCapabilityEvidence
            };
        report.execution_allowed = report.status.allows_execution();
        report
    }

    #[must_use]
    fn base(request: &PlanImportRequest) -> Self {
        Self {
            schema_version: "shardloom.imported_plan_capability_gate.v1",
            report_id: format!("imported-plan-capability-gate-{}", request.format.as_str()),
            status: ImportedPlanCapabilityGateStatus::NoImportedPlan,
            imported_plan_id: None,
            imported_plan_node_count: 0,
            capability_checked: false,
            required_capability_surfaces: Vec::new(),
            certified_capability_surfaces: Vec::new(),
            missing_certification_surfaces: Vec::new(),
            unsupported_node_count: 0,
            effect_boundary_count: 0,
            execution_allowed: false,
            runtime_execution: false,
            parser_executed: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            external_engine_execution: false,
            read_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.execution_allowed
            || self.runtime_execution
            || self.parser_executed
            || self.filesystem_probe
            || self.network_probe
            || self.catalog_probe
            || self.adapter_probe
            || self.external_engine_execution
            || self.read_io
            || self.write_io
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "imported_plan_capability_gate status={} capability_checked={} imported_plan_node_count={} execution_allowed={} missing_certification_surfaces={} fallback_execution_allowed=false",
            self.status.as_str(),
            self.capability_checked,
            self.imported_plan_node_count,
            self.execution_allowed,
            format_list(&self.missing_certification_surfaces)
        )
    }
}

fn imported_plan_required_surfaces(document: &NativePlanDocument) -> Vec<String> {
    let mut surfaces = BTreeSet::from(["native_plan_validation".to_string()]);
    for node in &document.nodes {
        for surface in imported_plan_node_required_surfaces(node.kind) {
            surfaces.insert((*surface).to_string());
        }
        for boundary in &node.boundaries {
            for surface in imported_plan_boundary_required_surfaces(*boundary) {
                surfaces.insert((*surface).to_string());
            }
        }
    }
    surfaces.into_iter().collect()
}

fn imported_plan_node_required_surfaces(kind: NativePlanNodeKind) -> &'static [&'static str] {
    match kind {
        NativePlanNodeKind::Scan => &["adapter_certification", "native_io_certificate_coverage"],
        NativePlanNodeKind::Filter
        | NativePlanNodeKind::Projection
        | NativePlanNodeKind::Aggregate
        | NativePlanNodeKind::Join
        | NativePlanNodeKind::Sort
        | NativePlanNodeKind::Limit => &["operator_coverage", "execution_certificate_coverage"],
        NativePlanNodeKind::Udf
        | NativePlanNodeKind::ModelCall
        | NativePlanNodeKind::EmbeddingGeneration
        | NativePlanNodeKind::VectorSearch => &[
            "function_coverage",
            "extension_safety",
            "security_governance",
        ],
        NativePlanNodeKind::ExternalRead
        | NativePlanNodeKind::ExternalWrite
        | NativePlanNodeKind::Translation => &[
            "adapter_certification",
            "native_io_certificate_coverage",
            "security_governance",
        ],
        NativePlanNodeKind::Write | NativePlanNodeKind::Commit => &[
            "adapter_certification",
            "native_io_certificate_coverage",
            "execution_certificate_coverage",
        ],
        NativePlanNodeKind::Unsupported => &["unsupported_node_rewrite"],
    }
}

fn imported_plan_boundary_required_surfaces(boundary: PlanBoundaryKind) -> &'static [&'static str] {
    match boundary {
        PlanBoundaryKind::NativeVortexInput | PlanBoundaryKind::NativeVortexOutput => {
            &["native_io_certificate_coverage"]
        }
        PlanBoundaryKind::CompatibilityOutput | PlanBoundaryKind::Translation => {
            &["adapter_certification", "semantic_profile_coverage"]
        }
        PlanBoundaryKind::Effect => &["extension_safety", "security_governance"],
        PlanBoundaryKind::Materialization | PlanBoundaryKind::ZeroCopyInterop => &[
            "native_io_certificate_coverage",
            "materialization_boundary_evidence",
        ],
        PlanBoundaryKind::ZeroDecode => &["native_io_certificate_coverage"],
        PlanBoundaryKind::Spill | PlanBoundaryKind::Shuffle | PlanBoundaryKind::Distributed => &[
            "operator_coverage",
            "execution_certificate_coverage",
            "memory_spill",
        ],
        PlanBoundaryKind::Unsupported => &["unsupported_boundary_rewrite"],
    }
}

fn imported_plan_surface_certified(
    certification: &CapabilityCertificationReport,
    surface: &str,
) -> bool {
    match surface {
        "adapter_certification" => {
            !certification.adapter_certification.entries.is_empty()
                && certification
                    .adapter_certification
                    .entries
                    .iter()
                    .all(|entry| entry.status == CapabilityCertificationStatus::Certified)
        }
        "function_coverage" => {
            !certification.function_coverage.entries.is_empty()
                && certification
                    .function_coverage
                    .entries
                    .iter()
                    .all(|entry| entry.status == CapabilityCertificationStatus::Certified)
        }
        "operator_coverage" => {
            !certification.operator_coverage.entries.is_empty()
                && certification
                    .operator_coverage
                    .entries
                    .iter()
                    .all(|entry| entry.status == OperatorCertificationStatus::ProductionCertified)
        }
        "semantic_profile_coverage" => {
            !certification.semantic_profiles.is_empty()
                && certification
                    .semantic_profiles
                    .iter()
                    .all(|entry| entry.status == CapabilityCertificationStatus::Certified)
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanExportStatus {
    Planned,
    Serialized,
    Unsupported,
    NotImplemented,
}
impl PlanExportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Serialized => "serialized",
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
    pub serialized_document: Option<String>,
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
            serialized_document: None,
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
    pub fn serialized_native(document: &NativePlanDocument) -> Self {
        let mut s = Self::new(PlanInteropFormat::ShardLoomNative);
        s.status = PlanExportStatus::Serialized;
        s.serialized_document = Some(document.to_native_serialized());
        s
    }
    #[must_use]
    pub fn not_implemented(format: PlanInteropFormat) -> Self {
        let mut s = Self::new(format);
        s.status = PlanExportStatus::NotImplemented;
        s.add_diagnostic(Diagnostic::not_implemented(
            "plan_export",
            "Plan export for the requested format is not implemented.",
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

    #[must_use]
    pub fn to_native_serialized(&self) -> String {
        let mut lines = vec![
            "shardloom.native_plan.v1".to_string(),
            format!("id={}", encode_plan_token(self.id.as_str())),
            format!(
                "schema_name={}",
                encode_plan_token(&self.schema_version.name)
            ),
            format!("schema_version={}", self.schema_version.version),
            format!("layer={}", self.layer.as_str()),
        ];
        for node in &self.nodes {
            let capabilities = node
                .capabilities
                .iter()
                .map(|capability| {
                    format!(
                        "{}:{}:{}",
                        capability.kind.as_str(),
                        u8::from(capability.required),
                        encode_plan_token(&capability.reason)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let boundaries = node
                .boundaries
                .iter()
                .map(PlanBoundaryKind::as_str)
                .collect::<Vec<_>>()
                .join(",");
            lines.push(format!(
                "node={}|{}|{}|{}|{}|{}",
                encode_plan_token(node.id.as_str()),
                node.layer.as_str(),
                node.kind.as_str(),
                encode_plan_token(&node.label),
                capabilities,
                boundaries
            ));
        }
        lines.join("\n")
    }

    /// Parses a ShardLoom-native serialized plan payload without IO or execution.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the payload is empty,
    /// malformed, or contains unknown native plan enum values.
    pub fn from_native_serialized(payload: &str) -> Result<Self> {
        if payload.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "native plan serialization payload must not be empty".to_string(),
            ));
        }

        let mut lines = payload.lines();
        if lines.next() != Some("shardloom.native_plan.v1") {
            return Err(ShardLoomError::InvalidOperation(
                "native plan serialization must start with shardloom.native_plan.v1".to_string(),
            ));
        }

        let mut id = None;
        let mut schema_name = None;
        let mut schema_version = None;
        let mut layer = None;
        let mut nodes = Vec::new();

        for line in lines {
            if let Some(value) = line.strip_prefix("id=") {
                id = Some(decode_plan_token(value));
            } else if let Some(value) = line.strip_prefix("schema_name=") {
                schema_name = Some(decode_plan_token(value));
            } else if let Some(value) = line.strip_prefix("schema_version=") {
                schema_version = Some(value.parse::<u32>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "invalid native plan schema version: {error}"
                    ))
                })?);
            } else if let Some(value) = line.strip_prefix("layer=") {
                layer = Some(parse_plan_layer(value)?);
            } else if let Some(value) = line.strip_prefix("node=") {
                nodes.push(parse_serialized_native_plan_node(value)?);
            } else if !line.trim().is_empty() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown native plan serialization line: {line}"
                )));
            }
        }

        let mut document = Self::new(
            PlanId::new(id.ok_or_else(|| {
                ShardLoomError::InvalidOperation("native plan serialization missing id".to_string())
            })?)?,
            layer.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native plan serialization missing layer".to_string(),
                )
            })?,
        );
        document.schema_version = PlanSchemaVersion::new(
            schema_name.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native plan serialization missing schema_name".to_string(),
                )
            })?,
            schema_version.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native plan serialization missing schema_version".to_string(),
                )
            })?,
        )?;
        document.nodes = nodes;
        document.validate_skeleton();
        Ok(document)
    }
}

fn encode_plan_token(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('\n', "%0A")
        .replace('\r', "%0D")
        .replace('|', "%7C")
        .replace(',', "%2C")
        .replace(':', "%3A")
        .replace('=', "%3D")
}

fn decode_plan_token(value: &str) -> String {
    value
        .replace("%0A", "\n")
        .replace("%0D", "\r")
        .replace("%7C", "|")
        .replace("%2C", ",")
        .replace("%3A", ":")
        .replace("%3D", "=")
        .replace("%25", "%")
}

fn parse_serialized_native_plan_node(value: &str) -> Result<NativePlanNode> {
    let parts = value.split('|').collect::<Vec<_>>();
    if parts.len() != 6 {
        return Err(ShardLoomError::InvalidOperation(
            "native plan node serialization must have six fields".to_string(),
        ));
    }
    let mut node = NativePlanNode::new(
        crate::PlanNodeId::new(decode_plan_token(parts[0]))?,
        parse_plan_layer(parts[1])?,
        parse_native_plan_node_kind(parts[2])?,
        decode_plan_token(parts[3]),
    );
    if !parts[4].is_empty() {
        for capability in parts[4].split(',') {
            let fields = capability.splitn(3, ':').collect::<Vec<_>>();
            if fields.len() != 3 {
                return Err(ShardLoomError::InvalidOperation(
                    "native plan capability serialization must have three fields".to_string(),
                ));
            }
            let required = match fields[1] {
                "1" => true,
                "0" => false,
                other => {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "invalid native plan capability required flag: {other}"
                    )));
                }
            };
            node.add_capability(PlanCapabilityRequirement {
                kind: parse_plan_capability_kind(fields[0])?,
                required,
                reason: decode_plan_token(fields[2]),
            });
        }
    }
    if !parts[5].is_empty() {
        for boundary in parts[5].split(',') {
            node.add_boundary(parse_plan_boundary_kind(boundary)?);
        }
    }
    Ok(node)
}

fn parse_plan_layer(value: &str) -> Result<PlanLayer> {
    match value {
        "user_intent" => Ok(PlanLayer::UserIntent),
        "logical" => Ok(PlanLayer::Logical),
        "optimized_logical" => Ok(PlanLayer::OptimizedLogical),
        "physical" => Ok(PlanLayer::Physical),
        "encoded_physical" => Ok(PlanLayer::EncodedPhysical),
        "streaming" => Ok(PlanLayer::Streaming),
        "runtime_task_graph" => Ok(PlanLayer::RuntimeTaskGraph),
        "adaptive_runtime" => Ok(PlanLayer::AdaptiveRuntime),
        "executed_report" => Ok(PlanLayer::ExecutedReport),
        "unsupported" => Ok(PlanLayer::Unsupported),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "unknown native plan layer: {other}"
        ))),
    }
}

fn parse_native_plan_node_kind(value: &str) -> Result<NativePlanNodeKind> {
    match value {
        "scan" => Ok(NativePlanNodeKind::Scan),
        "filter" => Ok(NativePlanNodeKind::Filter),
        "projection" => Ok(NativePlanNodeKind::Projection),
        "aggregate" => Ok(NativePlanNodeKind::Aggregate),
        "join" => Ok(NativePlanNodeKind::Join),
        "sort" => Ok(NativePlanNodeKind::Sort),
        "limit" => Ok(NativePlanNodeKind::Limit),
        "udf" => Ok(NativePlanNodeKind::Udf),
        "external_read" => Ok(NativePlanNodeKind::ExternalRead),
        "external_write" => Ok(NativePlanNodeKind::ExternalWrite),
        "model_call" => Ok(NativePlanNodeKind::ModelCall),
        "embedding_generation" => Ok(NativePlanNodeKind::EmbeddingGeneration),
        "vector_search" => Ok(NativePlanNodeKind::VectorSearch),
        "translation" => Ok(NativePlanNodeKind::Translation),
        "write" => Ok(NativePlanNodeKind::Write),
        "commit" => Ok(NativePlanNodeKind::Commit),
        "unsupported" => Ok(NativePlanNodeKind::Unsupported),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "unknown native plan node kind: {other}"
        ))),
    }
}

fn parse_plan_boundary_kind(value: &str) -> Result<PlanBoundaryKind> {
    match value {
        "native_vortex_input" => Ok(PlanBoundaryKind::NativeVortexInput),
        "native_vortex_output" => Ok(PlanBoundaryKind::NativeVortexOutput),
        "compatibility_output" => Ok(PlanBoundaryKind::CompatibilityOutput),
        "effect" => Ok(PlanBoundaryKind::Effect),
        "translation" => Ok(PlanBoundaryKind::Translation),
        "materialization" => Ok(PlanBoundaryKind::Materialization),
        "zero_decode" => Ok(PlanBoundaryKind::ZeroDecode),
        "zero_copy_boundary" => Ok(PlanBoundaryKind::ZeroCopyInterop),
        "spill" => Ok(PlanBoundaryKind::Spill),
        "shuffle" => Ok(PlanBoundaryKind::Shuffle),
        "distributed" => Ok(PlanBoundaryKind::Distributed),
        "unsupported" => Ok(PlanBoundaryKind::Unsupported),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "unknown native plan boundary kind: {other}"
        ))),
    }
}

fn parse_plan_capability_kind(value: &str) -> Result<PlanCapabilityKind> {
    match value {
        "vortex_native_input" => Ok(PlanCapabilityKind::VortexNativeInput),
        "vortex_native_output" => Ok(PlanCapabilityKind::VortexNativeOutput),
        "statistics" => Ok(PlanCapabilityKind::Statistics),
        "byte_ranges" => Ok(PlanCapabilityKind::ByteRanges),
        "encoded_kernel" => Ok(PlanCapabilityKind::EncodedKernel),
        "partial_decode" => Ok(PlanCapabilityKind::PartialDecode),
        "materialization" => Ok(PlanCapabilityKind::Materialization),
        "streaming" => Ok(PlanCapabilityKind::Streaming),
        "spill_support" => Ok(PlanCapabilityKind::SpillSupport),
        "object_store_access" => Ok(PlanCapabilityKind::ObjectStoreAccess),
        "external_credentials" => Ok(PlanCapabilityKind::ExternalCredentials),
        "explicit_effect_enablement" => Ok(PlanCapabilityKind::ExplicitEffectEnablement),
        "compatibility_output" => Ok(PlanCapabilityKind::CompatibilityOutput),
        "native_execution" => Ok(PlanCapabilityKind::NativeExecution),
        "unsupported" => Ok(PlanCapabilityKind::Unsupported),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "unknown native plan capability kind: {other}"
        ))),
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
    fn node_kind_write_and_commit_are_effectful() {
        assert!(NativePlanNodeKind::Write.is_effectful());
        assert!(NativePlanNodeKind::Commit.is_effectful());
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
    fn import_not_implemented_still_requires_native_validation() {
        let request =
            PlanImportRequest::not_implemented(PlanInteropFormat::JsonLike, "src").expect("ok");
        assert!(request.status.requires_validation());
        assert!(request.summary().contains("validation_required=true"));
    }
    #[test]
    fn portability_report_for_import_is_validation_only_and_no_fallback() {
        let request =
            PlanImportRequest::not_implemented(PlanInteropFormat::SubstraitLike, "fixture")
                .expect("request");
        let report = PlanPortabilityReport::for_import_request(&request);

        assert_eq!(report.schema_version, "shardloom.plan_portability.v1");
        assert_eq!(report.direction, PlanPortabilityDirection::ImportValidation);
        assert_eq!(report.status, PlanPortabilityStatus::NotImplemented);
        assert!(report.native_first);
        assert!(report.validation_only);
        assert!(report.validation_required);
        assert!(report.capability_check_required);
        assert!(
            report
                .unsupported_nodes
                .contains(&"real_plan_import".to_string())
        );
        assert!(
            report
                .residual_unsupported_constructs
                .contains(&"native_lowering".to_string())
        );
        assert!(!report.parser_executed);
        assert!(!report.runtime_execution);
        assert!(!report.external_engine_execution);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
    }
    #[test]
    fn native_plan_serialization_roundtrips_without_side_effects() {
        let mut document =
            NativePlanDocument::new(PlanId::new("roundtrip").expect("id"), PlanLayer::Logical);
        let mut scan = NativePlanNode::new(
            PlanNodeId::new("scan_0").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan | encoded",
        );
        scan.add_capability(PlanCapabilityRequirement::required(
            PlanCapabilityKind::VortexNativeInput,
            "preserve native capability: no fallback",
        ));
        scan.add_boundary(PlanBoundaryKind::NativeVortexInput);
        document.add_node(scan);
        document.validate_skeleton();

        let serialized = document.to_native_serialized();
        let imported =
            NativePlanDocument::from_native_serialized(&serialized).expect("roundtrip import");

        assert_eq!(imported.id.as_str(), "roundtrip");
        assert_eq!(imported.node_count(), 1);
        assert_eq!(imported.nodes[0].label, "scan | encoded");
        assert_eq!(imported.nodes[0].capabilities.len(), 1);
        assert_eq!(
            imported.nodes[0].capabilities[0].kind,
            PlanCapabilityKind::VortexNativeInput
        );
        assert_eq!(
            imported.nodes[0].boundaries,
            vec![PlanBoundaryKind::NativeVortexInput]
        );
        assert_eq!(imported.validation.status, PlanValidationStatus::Valid);
    }
    #[test]
    fn native_plan_serialization_rejects_unknown_prefix() {
        assert!(NativePlanDocument::from_native_serialized("other\nid=x").is_err());
    }
    #[test]
    fn native_import_request_records_imported_document() {
        let mut document =
            NativePlanDocument::new(PlanId::new("imported").expect("id"), PlanLayer::Logical);
        document.add_node(NativePlanNode::new(
            PlanNodeId::new("scan_0").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan",
        ));
        document.validate_skeleton();

        let request = PlanImportRequest::from_native_serialized(document.to_native_serialized())
            .expect("import request");
        let report = PlanPortabilityReport::for_import_request(&request);

        assert_eq!(request.status, PlanImportStatus::Imported);
        assert!(request.imported_document.is_some());
        assert!(request.serialization_performed);
        assert_eq!(report.status, PlanPortabilityStatus::Imported);
        assert!(report.import_export_serialization_performed);
        assert_eq!(
            report.substrait_like_representable_nodes,
            vec!["scan_0:scan".to_string()]
        );
        assert!(!report.has_errors());
    }
    #[test]
    fn imported_plan_capability_gate_blocks_missing_certification() {
        let mut document =
            NativePlanDocument::new(PlanId::new("imported").expect("id"), PlanLayer::Logical);
        document.add_node(NativePlanNode::new(
            PlanNodeId::new("scan_0").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan",
        ));
        document.validate_skeleton();
        let request = PlanImportRequest::from_native_serialized(document.to_native_serialized())
            .expect("native import");
        let gate = ImportedPlanCapabilityGateReport::for_import_request(
            &request,
            &CapabilityCertificationReport::contract_only(),
        );

        assert_eq!(
            gate.status,
            ImportedPlanCapabilityGateStatus::BlockedMissingCapabilityEvidence
        );
        assert!(gate.capability_checked);
        assert!(!gate.execution_allowed);
        assert_eq!(gate.imported_plan_id.as_deref(), Some("imported"));
        assert!(
            gate.required_capability_surfaces
                .contains(&"adapter_certification".to_string())
        );
        assert!(
            gate.required_capability_surfaces
                .contains(&"native_io_certificate_coverage".to_string())
        );
        assert!(
            gate.missing_certification_surfaces
                .contains(&"native_plan_validation".to_string())
        );
        assert!(!gate.runtime_execution);
        assert!(!gate.external_engine_execution);
        assert!(!gate.fallback_attempted);
    }
    #[test]
    fn imported_plan_capability_gate_blocks_effect_boundaries_first() {
        let mut document =
            NativePlanDocument::new(PlanId::new("effectful").expect("id"), PlanLayer::Logical);
        let mut udf = NativePlanNode::new(
            PlanNodeId::new("udf_0").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Udf,
            "python udf",
        );
        udf.add_boundary(PlanBoundaryKind::Effect);
        document.add_node(udf);
        document.validate_skeleton();
        let request = PlanImportRequest::from_native_serialized(document.to_native_serialized())
            .expect("native import");
        let gate = ImportedPlanCapabilityGateReport::for_import_request(
            &request,
            &CapabilityCertificationReport::contract_only(),
        );

        assert_eq!(
            gate.status,
            ImportedPlanCapabilityGateStatus::BlockedEffectBoundary
        );
        assert_eq!(gate.effect_boundary_count, 1);
        assert!(!gate.execution_allowed);
    }
    #[test]
    fn portability_report_classifies_native_nodes_and_boundaries() {
        let mut document = NativePlanDocument::empty(PlanId::new("p").expect("id"));
        let mut scan = NativePlanNode::new(
            PlanNodeId::new("n1").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan",
        );
        scan.add_boundary(PlanBoundaryKind::CompatibilityOutput);
        document.add_node(scan);
        let udf = NativePlanNode::new(
            PlanNodeId::new("n2").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Udf,
            "udf",
        );
        document.add_node(udf);
        document.validate_skeleton();

        let report = PlanPortabilityReport::native_skeleton(&document);

        assert!(
            report
                .substrait_like_representable_nodes
                .contains(&"n1:scan".to_string())
        );
        assert!(report.native_only_nodes.contains(&"n2:udf".to_string()));
        assert!(
            report
                .metadata_loss_boundaries
                .contains(&"n1:compatibility_output".to_string())
        );
        assert!(!report.has_errors());
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
    fn portability_report_for_export_records_redaction_and_no_side_effects() {
        let request =
            PlanExportRequest::not_implemented(PlanInteropFormat::JsonLike).include_secrets(true);
        let report = PlanPortabilityReport::for_export_request(&request);

        assert_eq!(report.direction, PlanPortabilityDirection::ExportValidation);
        assert_eq!(report.status, PlanPortabilityStatus::NotImplemented);
        assert!(report.redaction_required);
        assert!(
            report
                .metadata_loss_boundaries
                .contains(&"secret_redaction_boundary".to_string())
        );
        assert!(!report.import_export_serialization_performed);
        assert!(!report.read_io);
        assert!(!report.write_io);
        assert!(!report.fallback_attempted);
    }
    #[test]
    fn native_export_request_records_serialized_document() {
        let mut document =
            NativePlanDocument::new(PlanId::new("exported").expect("id"), PlanLayer::Logical);
        document.add_node(NativePlanNode::new(
            PlanNodeId::new("scan_0").expect("node id"),
            PlanLayer::Logical,
            NativePlanNodeKind::Scan,
            "scan",
        ));
        document.validate_skeleton();

        let request = PlanExportRequest::serialized_native(&document);
        let report = PlanPortabilityReport::for_export_request(&request);

        assert_eq!(request.status, PlanExportStatus::Serialized);
        assert!(
            request
                .serialized_document
                .as_deref()
                .is_some_and(|payload| payload.starts_with("shardloom.native_plan.v1"))
        );
        assert_eq!(report.status, PlanPortabilityStatus::Serialized);
        assert!(report.import_export_serialization_performed);
        assert!(!report.has_errors());
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
