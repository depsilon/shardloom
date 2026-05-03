use std::fmt::Write as _;

use crate::{
    CredentialScope, DatasetFormat, DatasetRef, DatasetUri, Diagnostic, DiagnosticCode,
    DiagnosticSeverity, Result, ShardLoomError,
};

/// Stable identifier for a universal input source contract in `ShardLoom`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InputSourceId(String);
impl InputSourceId {
    /// Creates a validated input source id.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "input source id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputSourceKind {
    NativeVortex,
    VortexFile,
    Parquet,
    ArrowIpc,
    ArrowLikeBoundary,
    Csv,
    JsonLines,
    IcebergCompatible,
    DeltaCompatible,
    LocalManifest,
    ObjectStoreManifest,
    CatalogTable,
    Api,
    Llm,
    Embedding,
    VectorIndex,
    UnstructuredText,
    BinaryBlob,
    InMemory,
    Unknown,
}
impl InputSourceKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortex => "native_vortex",
            Self::VortexFile => "vortex_file",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::ArrowLikeBoundary => "arrow_like_boundary",
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::LocalManifest => "local_manifest",
            Self::ObjectStoreManifest => "object_store_manifest",
            Self::CatalogTable => "catalog_table",
            Self::Api => "api",
            Self::Llm => "llm",
            Self::Embedding => "embedding",
            Self::VectorIndex => "vector_index",
            Self::UnstructuredText => "unstructured_text",
            Self::BinaryBlob => "binary_blob",
            Self::InMemory => "in_memory",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_native_vortex(&self) -> bool {
        matches!(self, Self::NativeVortex | Self::VortexFile)
    }
    #[must_use]
    pub const fn is_compatibility_structured(&self) -> bool {
        matches!(
            self,
            Self::Parquet
                | Self::ArrowIpc
                | Self::ArrowLikeBoundary
                | Self::Csv
                | Self::JsonLines
                | Self::IcebergCompatible
                | Self::DeltaCompatible
        )
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        matches!(
            self,
            Self::Api | Self::Llm | Self::Embedding | Self::VectorIndex
        )
    }
    #[must_use]
    pub const fn requires_credentials(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreManifest
                | Self::CatalogTable
                | Self::Api
                | Self::Llm
                | Self::Embedding
                | Self::VectorIndex
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputAdapterKind {
    NativeVortexAdapter,
    CompatibilityFileAdapter,
    ArrowBoundaryAdapter,
    CatalogAdapter,
    ManifestAdapter,
    EffectfulAdapter,
    UnstructuredAdapter,
    InMemoryAdapter,
    Unsupported,
}
impl InputAdapterKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexAdapter => "native_vortex_adapter",
            Self::CompatibilityFileAdapter => "compatibility_file_adapter",
            Self::ArrowBoundaryAdapter => "arrow_boundary_adapter",
            Self::CatalogAdapter => "catalog_adapter",
            Self::ManifestAdapter => "manifest_adapter",
            Self::EffectfulAdapter => "effectful_adapter",
            Self::UnstructuredAdapter => "unstructured_adapter",
            Self::InMemoryAdapter => "in_memory_adapter",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        matches!(self, Self::EffectfulAdapter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputCapabilityStatus {
    Supported,
    Planned,
    FeatureGated,
    RequiresConfiguration,
    RequiresCredentials,
    RequiresExplicitEnablement,
    Disabled,
    Unsupported,
}
impl InputCapabilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Planned => "planned",
            Self::FeatureGated => "feature_gated",
            Self::RequiresConfiguration => "requires_configuration",
            Self::RequiresCredentials => "requires_credentials",
            Self::RequiresExplicitEnablement => "requires_explicit_enablement",
            Self::Disabled => "disabled",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_usable_now(&self) -> bool {
        matches!(self, Self::Supported)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMetadataAvailability {
    Available,
    PartiallyAvailable,
    Deferred,
    Unavailable,
    Unknown,
}
impl InputMetadataAvailability {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::PartiallyAvailable => "partially_available",
            Self::Deferred => "deferred",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available | Self::PartiallyAvailable)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFidelityLevel {
    NativeFullFidelity,
    NativePartialFidelity,
    CompatibilityLogical,
    CompatibilityLossyPhysical,
    UnstructuredExtraction,
    EffectfulGenerated,
    Unsupported,
    Unknown,
}
impl InputFidelityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeFullFidelity => "native_full_fidelity",
            Self::NativePartialFidelity => "native_partial_fidelity",
            Self::CompatibilityLogical => "compatibility_logical",
            Self::CompatibilityLossyPhysical => "compatibility_lossy_physical",
            Self::UnstructuredExtraction => "unstructured_extraction",
            Self::EffectfulGenerated => "effectful_generated",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_native(&self) -> bool {
        matches!(self, Self::NativeFullFidelity | Self::NativePartialFidelity)
    }
    #[must_use]
    pub const fn is_lossy(&self) -> bool {
        matches!(
            self,
            Self::CompatibilityLossyPhysical
                | Self::UnstructuredExtraction
                | Self::EffectfulGenerated
                | Self::Unsupported
                | Self::Unknown
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMaterializationRisk {
    None,
    Low,
    Medium,
    High,
    Required,
    Unknown,
}
impl InputMaterializationRisk {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Required => "required",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_materialization(&self) -> bool {
        matches!(self, Self::Required)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputEffectLevel {
    None,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    EmbeddingCall,
    VectorSearch,
    Unknown,
}
impl InputEffectLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::EmbeddingCall => "embedding_call",
            Self::VectorSearch => "vector_search",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UniversalInputSource {
    pub id: InputSourceId,
    pub uri: Option<DatasetUri>,
    pub source_kind: InputSourceKind,
    pub adapter_kind: InputAdapterKind,
    pub dataset_format: DatasetFormat,
    pub credential_scope: Option<CredentialScope>,
    pub diagnostics: Vec<Diagnostic>,
}
impl UniversalInputSource {
    #[must_use]
    pub fn new(id: InputSourceId, source_kind: InputSourceKind) -> Self {
        Self {
            id,
            uri: None,
            source_kind,
            adapter_kind: InputAdapterKind::Unsupported,
            dataset_format: DatasetFormat::Unknown,
            credential_scope: None,
            diagnostics: vec![],
        }
    }
    /// # Errors
    /// Returns an error when the inferred `InputSourceId` is invalid.
    pub fn from_dataset_uri(uri: DatasetUri) -> Result<Self> {
        let id = InputSourceId::new(uri.as_str())?;
        let format = DatasetFormat::infer_from_uri(&uri);
        let (source_kind, adapter_kind) = match format {
            DatasetFormat::Vortex => (
                InputSourceKind::VortexFile,
                InputAdapterKind::NativeVortexAdapter,
            ),
            DatasetFormat::Parquet => (
                InputSourceKind::Parquet,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::ArrowIpc => (
                InputSourceKind::ArrowIpc,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::Csv => (
                InputSourceKind::Csv,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::JsonLines => (
                InputSourceKind::JsonLines,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::IcebergCompatible => (
                InputSourceKind::IcebergCompatible,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::DeltaCompatible => (
                InputSourceKind::DeltaCompatible,
                InputAdapterKind::CompatibilityFileAdapter,
            ),
            DatasetFormat::Unknown | DatasetFormat::Extension(_) => {
                (InputSourceKind::Unknown, InputAdapterKind::Unsupported)
            }
        };
        Ok(Self {
            id,
            uri: Some(uri),
            source_kind,
            adapter_kind,
            dataset_format: format,
            credential_scope: None,
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn with_credential_scope(mut self, scope: CredentialScope) -> Self {
        self.credential_scope = Some(scope);
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub const fn is_native_vortex(&self) -> bool {
        self.source_kind.is_native_vortex()
    }
    #[must_use]
    pub const fn requires_credentials(&self) -> bool {
        self.source_kind.requires_credentials()
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
    pub fn summary(&self) -> String {
        format!(
            "id={}; kind={}; format={}",
            self.id.as_str(),
            self.source_kind.as_str(),
            self.dataset_format.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputCapability {
    pub name: String,
    pub status: InputCapabilityStatus,
    pub notes: Option<String>,
}
impl InputCapability {
    /// # Errors
    pub fn new(name: impl Into<String>, status: InputCapabilityStatus) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "input capability name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
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
    pub const fn is_usable_now(&self) -> bool {
        self.status.is_usable_now()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}: {}", self.name, self.status.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct InputAdapterReport {
    pub source: UniversalInputSource,
    pub capability_status: InputCapabilityStatus,
    pub metadata_availability: InputMetadataAvailability,
    pub fidelity: InputFidelityLevel,
    pub materialization_risk: InputMaterializationRisk,
    pub effect_level: InputEffectLevel,
    pub capabilities: Vec<InputCapability>,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl InputAdapterReport {
    #[must_use]
    pub fn for_source(source: UniversalInputSource) -> Self {
        let (status, meta, fidelity, risk, effect) = if source.source_kind.is_native_vortex() {
            (
                InputCapabilityStatus::Planned,
                InputMetadataAvailability::Deferred,
                InputFidelityLevel::NativeFullFidelity,
                InputMaterializationRisk::Low,
                InputEffectLevel::None,
            )
        } else if source.source_kind.is_effectful() {
            (
                InputCapabilityStatus::RequiresExplicitEnablement,
                InputMetadataAvailability::Unknown,
                InputFidelityLevel::EffectfulGenerated,
                InputMaterializationRisk::Unknown,
                match source.source_kind {
                    InputSourceKind::Llm => InputEffectLevel::ModelCall,
                    InputSourceKind::Embedding => InputEffectLevel::EmbeddingCall,
                    InputSourceKind::VectorIndex => InputEffectLevel::VectorSearch,
                    _ => InputEffectLevel::ExternalRead,
                },
            )
        } else if source.source_kind.is_compatibility_structured() {
            (
                InputCapabilityStatus::Planned,
                InputMetadataAvailability::Deferred,
                InputFidelityLevel::CompatibilityLogical,
                InputMaterializationRisk::Medium,
                InputEffectLevel::None,
            )
        } else {
            (
                InputCapabilityStatus::Unsupported,
                InputMetadataAvailability::Unknown,
                InputFidelityLevel::Unknown,
                InputMaterializationRisk::Unknown,
                InputEffectLevel::Unknown,
            )
        };
        Self {
            source,
            capability_status: status,
            metadata_availability: meta,
            fidelity,
            materialization_risk: risk,
            effect_level: effect,
            capabilities: vec![],
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn unsupported(
        source: UniversalInputSource,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::for_source(source);
        s.capability_status = InputCapabilityStatus::Unsupported;
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Input source is unsupported for native execution.",
            Some(reason.into()),
        ));
        s
    }
    pub fn add_capability(&mut self, c: InputCapability) {
        self.capabilities.push(c);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
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
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "source: {}", self.source.summary());
        let _ = writeln!(out, "adapter: {}", self.source.adapter_kind.as_str());
        let _ = writeln!(
            out,
            "capability status: {}",
            self.capability_status.as_str()
        );
        let _ = writeln!(
            out,
            "metadata availability: {}",
            self.metadata_availability.as_str()
        );
        let _ = writeln!(out, "fidelity: {}", self.fidelity.as_str());
        let _ = writeln!(
            out,
            "materialization risk: {}",
            self.materialization_risk.as_str()
        );
        let _ = writeln!(out, "effect level: {}", self.effect_level.as_str());
        let _ = writeln!(
            out,
            "data read: false
data materialized: false
object-store io: false
external effects executed: false
fallback execution: disabled"
        );
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InputAdapterRegistrySnapshot {
    pub adapters: Vec<InputCapability>,
    pub diagnostics: Vec<Diagnostic>,
}
impl InputAdapterRegistrySnapshot {
    #[must_use]
    pub fn foundation() -> Self {
        let adapters = [
            ("native_vortex", InputCapabilityStatus::Planned),
            ("parquet", InputCapabilityStatus::Planned),
            ("arrow_ipc", InputCapabilityStatus::Planned),
            ("csv", InputCapabilityStatus::Planned),
            ("jsonl", InputCapabilityStatus::Planned),
            ("iceberg_compatible", InputCapabilityStatus::Planned),
            ("delta_compatible", InputCapabilityStatus::Planned),
            ("api", InputCapabilityStatus::RequiresExplicitEnablement),
            ("llm", InputCapabilityStatus::RequiresExplicitEnablement),
            (
                "embeddings",
                InputCapabilityStatus::RequiresExplicitEnablement,
            ),
            (
                "vector_index",
                InputCapabilityStatus::RequiresExplicitEnablement,
            ),
        ]
        .into_iter()
        .filter_map(|(n, s)| InputCapability::new(n, s).ok())
        .collect();
        Self {
            adapters,
            diagnostics: vec![],
        }
    }
    pub fn add_adapter(&mut self, a: InputCapability) {
        self.adapters.push(a);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub fn adapter_count(&self) -> usize {
        self.adapters.len()
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
    pub fn to_human_text(&self) -> String {
        let mut out = String::from("ShardLoom input adapter registry snapshot\n");
        for a in &self.adapters {
            let _ = writeln!(out, "- {}", a.summary());
        }
        out.push_str("fallback execution: disabled\n");
        out
    }
}

/// # Errors
pub fn input_source_to_dataset_ref(source: &UniversalInputSource) -> Result<Option<DatasetRef>> {
    source.uri.clone().map(DatasetRef::from_uri).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn input_source_id_rejects_empty_ids() {
        assert!(InputSourceId::new(" ").is_err());
    }
    #[test]
    fn source_kind_flags() {
        assert!(InputSourceKind::NativeVortex.is_native_vortex());
        assert!(InputSourceKind::Parquet.is_compatibility_structured());
        assert!(InputSourceKind::Api.is_effectful());
        assert!(InputSourceKind::Llm.requires_credentials() && InputSourceKind::Llm.is_effectful());
    }
    #[test]
    fn capability_checks() {
        assert!(InputCapability::new(" ", InputCapabilityStatus::Planned).is_err());
        assert!(InputCapabilityStatus::Supported.is_usable_now());
        assert!(!InputCapabilityStatus::Planned.is_usable_now());
    }
    #[test]
    fn from_uri_mapping() {
        let v = UniversalInputSource::from_dataset_uri(DatasetUri::new("x.vortex").expect("uri"))
            .expect("ok");
        assert!(v.is_native_vortex());
        let p = UniversalInputSource::from_dataset_uri(DatasetUri::new("x.parquet").expect("uri"))
            .expect("ok");
        assert!(p.source_kind.is_compatibility_structured());
    }
    #[test]
    fn from_uri_preserves_compatibility_source_kind() {
        let csv = UniversalInputSource::from_dataset_uri(DatasetUri::new("x.csv").expect("uri"))
            .expect("ok");
        assert_eq!(csv.source_kind, InputSourceKind::Csv);

        let jsonl =
            UniversalInputSource::from_dataset_uri(DatasetUri::new("x.jsonl").expect("uri"))
                .expect("ok");
        assert_eq!(jsonl.source_kind, InputSourceKind::JsonLines);

        let arrow =
            UniversalInputSource::from_dataset_uri(DatasetUri::new("x.arrow").expect("uri"))
                .expect("ok");
        assert_eq!(arrow.source_kind, InputSourceKind::ArrowIpc);
    }
    #[test]
    fn report_behaviors() {
        let v = UniversalInputSource::from_dataset_uri(DatasetUri::new("x.vortex").expect("uri"))
            .expect("ok");
        let rv = InputAdapterReport::for_source(v);
        assert!(rv.fidelity.is_native());
        let p = UniversalInputSource::from_dataset_uri(DatasetUri::new("x.parquet").expect("uri"))
            .expect("ok");
        let rp = InputAdapterReport::for_source(p);
        assert!(matches!(
            rp.materialization_risk,
            InputMaterializationRisk::Medium | InputMaterializationRisk::High
        ));
        let e =
            UniversalInputSource::new(InputSourceId::new("api").expect("id"), InputSourceKind::Api);
        let re = InputAdapterReport::for_source(e);
        assert!(!re.external_effects_executed);
        assert!(rp.is_side_effect_free());
    }
    #[test]
    fn unsupported_and_registry() {
        let src = UniversalInputSource::new(
            InputSourceId::new("u").expect("id"),
            InputSourceKind::Unknown,
        );
        let r = InputAdapterReport::unsupported(src, "feature", "reason");
        assert!(r.has_errors());
        assert!(!r.diagnostics[0].fallback.attempted);
        let reg = InputAdapterRegistrySnapshot::foundation();
        assert!(reg.adapters.iter().any(|a| a.name == "native_vortex"));
        assert!(reg.adapters.iter().any(|a| a.name == "parquet"));
        assert!(reg.to_human_text().contains("fallback execution: disabled"));
    }
    #[test]
    fn dataset_ref_conversion() {
        let src = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://a.vortex").expect("uri"),
        )
        .expect("ok");
        assert!(input_source_to_dataset_ref(&src).expect("ok").is_some());
        let src2 = UniversalInputSource::new(
            InputSourceId::new("x").expect("id"),
            InputSourceKind::InMemory,
        );
        assert!(input_source_to_dataset_ref(&src2).expect("ok").is_none());
    }
}
