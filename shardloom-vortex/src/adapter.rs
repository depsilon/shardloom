//! Upstream Vortex API discovery and adapter mapping skeleton.
//!
//! This module is planning-only: no file IO, no object-store IO, no decode-to-Arrow
//! default path, and no fallback execution engines.

use shardloom_core::{Diagnostic, DiagnosticCode, LogicalDType, Result, ShardLoomError};

/// Public API area categories discovered from upstream Vortex documentation/source inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexApiArea {
    DType,
    Array,
    Encoding,
    Layout,
    Statistics,
    FileIo,
    Scan,
    Write,
    ArrowInterop,
    Unknown,
}
impl VortexApiArea {
    /// Returns stable machine-readable area label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DType => "dtype",
            Self::Array => "array",
            Self::Encoding => "encoding",
            Self::Layout => "layout",
            Self::Statistics => "statistics",
            Self::FileIo => "file_io",
            Self::Scan => "scan",
            Self::Write => "write",
            Self::ArrowInterop => "arrow_interop",
            Self::Unknown => "unknown",
        }
    }
}

/// Support status for a discovered upstream API surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexApiSupportStatus {
    ConfirmedPublic,
    Planned,
    Deferred,
    NotConfirmed,
    Unsupported,
}
impl VortexApiSupportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ConfirmedPublic => "confirmed_public",
            Self::Planned => "planned",
            Self::Deferred => "deferred",
            Self::NotConfirmed => "not_confirmed",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_usable_now(&self) -> bool {
        matches!(self, Self::ConfirmedPublic)
    }
}

/// One item in ShardLoom's upstream Vortex API inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexApiInventoryItem {
    pub area: VortexApiArea,
    pub name: String,
    pub status: VortexApiSupportStatus,
    pub notes: Option<String>,
}
impl VortexApiInventoryItem {
    /// Creates an inventory item with required non-empty API name.
    pub fn new(
        area: VortexApiArea,
        name: impl Into<String>,
        status: VortexApiSupportStatus,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "vortex API inventory item name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            area,
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
        format!(
            "area={} name={} status={}",
            self.area.as_str(),
            self.name,
            self.status.as_str()
        )
    }
}

/// Adapter capability boundaries for staged Vortex integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexAdapterCapability {
    DependencyLinked,
    DTypeMapping,
    EncodingMapping,
    LayoutMapping,
    StatisticsMapping,
    MetadataInspection,
    ReadPlanning,
    OutputPlanning,
    ActualRead,
    ActualWrite,
}
impl VortexAdapterCapability {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DependencyLinked => "dependency_linked",
            Self::DTypeMapping => "dtype_mapping",
            Self::EncodingMapping => "encoding_mapping",
            Self::LayoutMapping => "layout_mapping",
            Self::StatisticsMapping => "statistics_mapping",
            Self::MetadataInspection => "metadata_inspection",
            Self::ReadPlanning => "read_planning",
            Self::OutputPlanning => "output_planning",
            Self::ActualRead => "actual_read",
            Self::ActualWrite => "actual_write",
        }
    }
    #[must_use]
    pub const fn requires_io(&self) -> bool {
        matches!(self, Self::ActualRead | Self::ActualWrite)
    }
}

/// Availability status for an adapter capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexAdapterCapabilityStatus {
    Available,
    Planned,
    BlockedOnApiDiscovery,
    BlockedOnIoImplementation,
    Unsupported,
}
impl VortexAdapterCapabilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Planned => "planned",
            Self::BlockedOnApiDiscovery => "blocked_on_api_discovery",
            Self::BlockedOnIoImplementation => "blocked_on_io_implementation",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }
}

/// Discovery-time capability report. This is diagnostics/reporting only.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexAdapterCapabilityReport {
    pub capabilities: Vec<(VortexAdapterCapability, VortexAdapterCapabilityStatus)>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexAdapterCapabilityReport {
    #[must_use]
    pub fn foundation() -> Self {
        Self {
            capabilities: vec![
                (
                    VortexAdapterCapability::DependencyLinked,
                    VortexAdapterCapabilityStatus::Available,
                ),
                (
                    VortexAdapterCapability::DTypeMapping,
                    VortexAdapterCapabilityStatus::BlockedOnApiDiscovery,
                ),
                (
                    VortexAdapterCapability::EncodingMapping,
                    VortexAdapterCapabilityStatus::BlockedOnApiDiscovery,
                ),
                (
                    VortexAdapterCapability::LayoutMapping,
                    VortexAdapterCapabilityStatus::BlockedOnApiDiscovery,
                ),
                (
                    VortexAdapterCapability::StatisticsMapping,
                    VortexAdapterCapabilityStatus::BlockedOnApiDiscovery,
                ),
                (
                    VortexAdapterCapability::MetadataInspection,
                    VortexAdapterCapabilityStatus::Planned,
                ),
                (
                    VortexAdapterCapability::ReadPlanning,
                    VortexAdapterCapabilityStatus::Planned,
                ),
                (
                    VortexAdapterCapability::OutputPlanning,
                    VortexAdapterCapabilityStatus::Planned,
                ),
                (
                    VortexAdapterCapability::ActualRead,
                    VortexAdapterCapabilityStatus::BlockedOnIoImplementation,
                ),
                (
                    VortexAdapterCapability::ActualWrite,
                    VortexAdapterCapabilityStatus::BlockedOnIoImplementation,
                ),
            ],
            diagnostics: vec![],
        }
    }
    pub fn add_capability(
        &mut self,
        capability: VortexAdapterCapability,
        status: VortexAdapterCapabilityStatus,
    ) {
        self.capabilities.push((capability, status));
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn is_available(&self, capability: VortexAdapterCapability) -> bool {
        self.capabilities
            .iter()
            .any(|(c, s)| *c == capability && s.is_available())
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.code,
                DiagnosticCode::InvalidInput
                    | DiagnosticCode::NotImplemented
                    | DiagnosticCode::NoFallbackExecution
            )
        })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::from(
            "Vortex API inventory\nfallback execution: disabled\nactual IO: not implemented",
        );
        for (cap, status) in &self.capabilities {
            out.push_str(&format!("\n- {}: {}", cap.as_str(), status.as_str()));
        }
        out
    }
}

/// Temporary name-based mapping helper until typed upstream DType mapping is confirmed.
#[must_use]
pub fn map_known_vortex_dtype_name(name: &str) -> LogicalDType {
    match name.trim().to_ascii_lowercase().as_str() {
        "bool" | "boolean" => LogicalDType::Boolean,
        "int64" | "i64" => LogicalDType::Int64,
        "uint64" | "u64" => LogicalDType::UInt64,
        "float64" | "f64" => LogicalDType::Float64,
        "utf8" | "string" => LogicalDType::Utf8,
        "binary" => LogicalDType::Binary,
        "date32" | "date" => LogicalDType::Date32,
        "timestamp" | "timestamp_micros" => LogicalDType::TimestampMicros,
        _ => LogicalDType::Unknown,
    }
}

/// Temporary name-based mapping helper until typed upstream encoding mapping is confirmed.
#[must_use]
pub fn map_known_vortex_encoding_name(name: &str) -> shardloom_core::EncodingKind {
    match name.trim().to_ascii_lowercase().as_str() {
        "plain" => shardloom_core::EncodingKind::Plain,
        "constant" | "const" => shardloom_core::EncodingKind::Constant,
        "dictionary" | "dict" => shardloom_core::EncodingKind::Dictionary,
        "runlength" | "rle" | "run_length" => shardloom_core::EncodingKind::RunLength,
        "delta" => shardloom_core::EncodingKind::Delta,
        "bitpacked" | "bit_packed" => shardloom_core::EncodingKind::BitPacked,
        "fsst" | "fsstlike" => shardloom_core::EncodingKind::FsstLike,
        "fastlanes" | "fast_lanes" => shardloom_core::EncodingKind::FastLanesLike,
        "alp" => shardloom_core::EncodingKind::AlpLike,
        _ => shardloom_core::EncodingKind::Unknown,
    }
}

/// Temporary name-based mapping helper until typed upstream layout mapping is confirmed.
#[must_use]
pub fn map_known_vortex_layout_name(name: &str) -> shardloom_core::LayoutKind {
    match name.trim().to_ascii_lowercase().as_str() {
        "flat" => shardloom_core::LayoutKind::Flat,
        "chunked" | "chunk" => shardloom_core::LayoutKind::Chunked,
        "struct" => shardloom_core::LayoutKind::Struct,
        "list" => shardloom_core::LayoutKind::List,
        "sparse" => shardloom_core::LayoutKind::Sparse,
        _ => shardloom_core::LayoutKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_item_rejects_empty_name() {
        assert!(
            VortexApiInventoryItem::new(
                VortexApiArea::DType,
                "   ",
                VortexApiSupportStatus::Planned
            )
            .is_err()
        );
    }
    #[test]
    fn support_confirmed_public_usable_now() {
        assert!(VortexApiSupportStatus::ConfirmedPublic.is_usable_now());
    }
    #[test]
    fn support_planned_not_usable_now() {
        assert!(!VortexApiSupportStatus::Planned.is_usable_now());
    }
    #[test]
    fn cap_actual_read_requires_io() {
        assert!(VortexAdapterCapability::ActualRead.requires_io());
    }
    #[test]
    fn cap_actual_write_requires_io() {
        assert!(VortexAdapterCapability::ActualWrite.requires_io());
    }
    #[test]
    fn cap_dtype_mapping_not_require_io() {
        assert!(!VortexAdapterCapability::DTypeMapping.requires_io());
    }
    #[test]
    fn status_available_is_available() {
        assert!(VortexAdapterCapabilityStatus::Available.is_available());
    }
    #[test]
    fn foundation_has_dependency_linked_available() {
        assert!(
            VortexAdapterCapabilityReport::foundation()
                .is_available(VortexAdapterCapability::DependencyLinked)
        );
    }
    #[test]
    fn foundation_actual_read_not_available() {
        assert!(
            !VortexAdapterCapabilityReport::foundation()
                .is_available(VortexAdapterCapability::ActualRead)
        );
    }
    #[test]
    fn human_text_mentions_fallback_disabled() {
        assert!(
            VortexAdapterCapabilityReport::foundation()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn map_dtype_bool_boolean() {
        assert_eq!(map_known_vortex_dtype_name("bool"), LogicalDType::Boolean);
        assert_eq!(
            map_known_vortex_dtype_name("boolean"),
            LogicalDType::Boolean
        );
    }
    #[test]
    fn map_dtype_utf8_string() {
        assert_eq!(map_known_vortex_dtype_name("utf8"), LogicalDType::Utf8);
        assert_eq!(map_known_vortex_dtype_name("string"), LogicalDType::Utf8);
    }
    #[test]
    fn map_dtype_unknown() {
        assert_eq!(map_known_vortex_dtype_name("??"), LogicalDType::Unknown);
    }
    #[test]
    fn map_encoding_dictionary_dict() {
        assert_eq!(
            map_known_vortex_encoding_name("dictionary"),
            shardloom_core::EncodingKind::Dictionary
        );
        assert_eq!(
            map_known_vortex_encoding_name("dict"),
            shardloom_core::EncodingKind::Dictionary
        );
    }
    #[test]
    fn map_encoding_rle_run_length() {
        assert_eq!(
            map_known_vortex_encoding_name("rle"),
            shardloom_core::EncodingKind::RunLength
        );
        assert_eq!(
            map_known_vortex_encoding_name("run_length"),
            shardloom_core::EncodingKind::RunLength
        );
    }
    #[test]
    fn map_encoding_unknown() {
        assert_eq!(
            map_known_vortex_encoding_name("??"),
            shardloom_core::EncodingKind::Unknown
        );
    }
    #[test]
    fn map_layout_flat() {
        assert_eq!(
            map_known_vortex_layout_name("flat"),
            shardloom_core::LayoutKind::Flat
        );
    }
    #[test]
    fn map_layout_chunked() {
        assert_eq!(
            map_known_vortex_layout_name("chunked"),
            shardloom_core::LayoutKind::Chunked
        );
    }
    #[test]
    fn map_layout_unknown() {
        assert_eq!(
            map_known_vortex_layout_name("??"),
            shardloom_core::LayoutKind::Unknown
        );
    }
}
