//! Upstream Vortex API discovery and adapter mapping skeleton.
//!
//! This module is planning-only: no file IO, no object-store IO, no decode-to-Arrow
//! default path, and no fallback execution engines.

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, LogicalDType, Result, ShardLoomError};

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

/// One item in `ShardLoom`'s upstream Vortex API inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexApiInventoryItem {
    pub area: VortexApiArea,
    pub name: String,
    pub status: VortexApiSupportStatus,
    pub notes: Option<String>,
}
impl VortexApiInventoryItem {
    /// Creates a Vortex API inventory item.
    ///
    /// # Errors
    ///
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or
    /// whitespace-only.
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
        self.diagnostics
            .iter()
            .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::from(
            "Vortex API inventory\nfallback execution: disabled\nactual IO: not implemented",
        );
        for (cap, status) in &self.capabilities {
            let _ = write!(out, "\n- {}: {}", cap.as_str(), status.as_str());
        }
        if self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics: none");
        } else {
            out.push_str("\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                let _ = write!(out, "\n- {}", diagnostic.to_human_text());
            }
        }
        out
    }
}

/// Typed `DType` mapping status for the Vortex adapter boundary.
///
/// This reports adapter capability only and does not perform Vortex IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexTypedMappingStatus {
    Implemented,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexTypedMappingStatus {
    /// Returns a stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::DeferredApiUnclear => "deferred_api_unclear",
            Self::DeferredApiUnstable => "deferred_api_unstable",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether typed mapping is currently implemented.
    #[must_use]
    pub const fn is_implemented(&self) -> bool {
        matches!(self, Self::Implemented)
    }
}

/// Reporting-only summary for the typed Vortex `DType` mapping probe.
///
/// This report is adapter-boundary metadata only: no IO, no Arrow-default decode path,
/// and no fallback execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexDTypeMappingReport {
    pub status: VortexTypedMappingStatus,
    pub typed_api_name: Option<String>,
    pub name_based_mapping_available: bool,
    pub actual_io_implemented: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexDTypeMappingReport {
    /// Creates a report for implemented typed mapping against a public API.
    #[must_use]
    pub fn implemented(typed_api_name: impl Into<String>) -> Self {
        Self {
            status: VortexTypedMappingStatus::Implemented,
            typed_api_name: Some(typed_api_name.into()),
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }

    /// Creates a report for deferred mapping when public API remains unclear.
    #[must_use]
    pub fn deferred_api_unclear() -> Self {
        Self {
            status: VortexTypedMappingStatus::DeferredApiUnclear,
            typed_api_name: None,
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }

    /// Creates a report for deferred mapping when public API is unstable.
    #[must_use]
    pub fn deferred_api_unstable() -> Self {
        Self {
            status: VortexTypedMappingStatus::DeferredApiUnstable,
            typed_api_name: None,
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }

    /// Appends a deterministic diagnostic message.
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Returns whether the report contains error/fatal diagnostics.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }

    /// Renders a human summary for CLI and operator diagnostics.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex typed DType mapping probe
status: {}
name-based mapping available: {}
actual IO implemented: {}
fallback execution allowed: {}",
            self.status.as_str(),
            self.name_based_mapping_available,
            self.actual_io_implemented,
            self.fallback_execution_allowed,
        );
        if let Some(api) = &self.typed_api_name {
            let _ = write!(
                out,
                "
typed API: {api}"
            );
        }
        if self.diagnostics.is_empty() {
            out.push_str(
                "
diagnostics: none",
            );
        } else {
            out.push_str(
                "
diagnostics:",
            );
            for diagnostic in &self.diagnostics {
                let _ = write!(out, "\n- {}", diagnostic.to_human_text());
                if let Some(reason) = &diagnostic.reason {
                    let _ = write!(out, "\n  reason: {reason}");
                }
                if let Some(next_step) = &diagnostic.suggested_next_step {
                    let _ = write!(out, "\n  suggested next step: {next_step}");
                }
            }
        }
        out
    }
}

/// Returns whether compile-safe typed upstream Vortex `DType` mapping is available.
///
/// This probe is currently deferred until a stable public typed `DType` API is
/// confirmed in this environment. No Vortex IO occurs.
#[must_use]
pub const fn typed_vortex_dtype_mapping_available() -> bool {
    false
}

/// Typed encoding mapping status at the Vortex adapter boundary in `ShardLoom`.
///
/// This status is compile-safe metadata only and never performs IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodingMappingStatus {
    Implemented,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexEncodingMappingStatus {
    /// Returns a stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::DeferredApiUnclear => "deferred_api_unclear",
            Self::DeferredApiUnstable => "deferred_api_unstable",
            Self::Unsupported => "unsupported",
        }
    }
    /// Returns whether typed mapping is currently implemented.
    #[must_use]
    pub const fn is_implemented(&self) -> bool {
        matches!(self, Self::Implemented)
    }
}

/// Typed layout mapping status at the Vortex adapter boundary in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLayoutMappingStatus {
    Implemented,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexLayoutMappingStatus {
    /// Returns a stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::DeferredApiUnclear => "deferred_api_unclear",
            Self::DeferredApiUnstable => "deferred_api_unstable",
            Self::Unsupported => "unsupported",
        }
    }
    /// Returns whether typed mapping is currently implemented.
    #[must_use]
    pub const fn is_implemented(&self) -> bool {
        matches!(self, Self::Implemented)
    }
}

/// Report for typed Vortex encoding/layout mapping probe at the `ShardLoom` adapter boundary.
///
/// No Vortex IO occurs, name-based mapping remains available, and fallback execution is disabled.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodingLayoutMappingReport {
    pub encoding_status: VortexEncodingMappingStatus,
    pub layout_status: VortexLayoutMappingStatus,
    pub encoding_api_name: Option<String>,
    pub layout_api_name: Option<String>,
    pub name_based_mapping_available: bool,
    pub actual_io_implemented: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodingLayoutMappingReport {
    #[must_use]
    pub fn implemented(
        encoding_api_name: impl Into<String>,
        layout_api_name: impl Into<String>,
    ) -> Self {
        Self {
            encoding_status: VortexEncodingMappingStatus::Implemented,
            layout_status: VortexLayoutMappingStatus::Implemented,
            encoding_api_name: Some(encoding_api_name.into()),
            layout_api_name: Some(layout_api_name.into()),
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn encoding_implemented_layout_deferred(encoding_api_name: impl Into<String>) -> Self {
        Self {
            encoding_status: VortexEncodingMappingStatus::Implemented,
            layout_status: VortexLayoutMappingStatus::DeferredApiUnclear,
            encoding_api_name: Some(encoding_api_name.into()),
            layout_api_name: None,
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn deferred_api_unclear() -> Self {
        Self {
            encoding_status: VortexEncodingMappingStatus::DeferredApiUnclear,
            layout_status: VortexLayoutMappingStatus::DeferredApiUnclear,
            encoding_api_name: None,
            layout_api_name: None,
            name_based_mapping_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn deferred_api_unstable() -> Self {
        Self {
            encoding_status: VortexEncodingMappingStatus::DeferredApiUnstable,
            layout_status: VortexLayoutMappingStatus::DeferredApiUnstable,
            ..Self::deferred_api_unclear()
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex typed encoding/layout mapping probe\nencoding mapping status: {}\nlayout mapping status: {}\nname-based mapping available: {}\nactual IO implemented: {}\nfallback execution allowed: {}",
            self.encoding_status.as_str(),
            self.layout_status.as_str(),
            self.name_based_mapping_available,
            self.actual_io_implemented,
            self.fallback_execution_allowed
        );
        if let Some(api) = &self.encoding_api_name {
            let _ = write!(out, "\nencoding API: {api}");
        }
        if let Some(api) = &self.layout_api_name {
            let _ = write!(out, "\nlayout API: {api}");
        }
        if self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics: none");
        } else {
            out.push_str("\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                let _ = write!(out, "\n- {}", diagnostic.to_human_text());
            }
        }
        out
    }
}

/// Temporary name-based mapping helper until typed upstream `DType` mapping is confirmed.
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
        "struct" => LogicalDType::Struct,
        "list" => LogicalDType::List,
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
        "fsst" | "fsstlike" | "fsst_like" => shardloom_core::EncodingKind::FsstLike,
        "fastlanes" | "fast_lanes" | "fastlanes_like" => {
            shardloom_core::EncodingKind::FastLanesLike
        }
        "alp" | "alp_like" => shardloom_core::EncodingKind::AlpLike,
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
    fn has_errors_false_without_diagnostics() {
        let report = VortexAdapterCapabilityReport::foundation();
        assert!(!report.has_errors());
    }

    #[test]
    fn has_errors_treats_any_error_severity_as_error() {
        let mut report = VortexAdapterCapabilityReport::foundation();
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_api_inventory",
            "invalid adapter config",
            "fix config",
        ));
        assert!(report.has_errors());
    }
    #[test]
    fn foundation_dtype_mapping_is_blocked_on_api_discovery() {
        let report = VortexAdapterCapabilityReport::foundation();
        assert!(report.capabilities.iter().any(|(capability, status)| {
            *capability == VortexAdapterCapability::DTypeMapping
                && *status == VortexAdapterCapabilityStatus::BlockedOnApiDiscovery
        }));
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
    fn map_dtype_struct_list() {
        assert_eq!(map_known_vortex_dtype_name("struct"), LogicalDType::Struct);
        assert_eq!(map_known_vortex_dtype_name("list"), LogicalDType::List);
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
    fn map_encoding_canonical_like_names() {
        assert_eq!(
            map_known_vortex_encoding_name("fsst_like"),
            shardloom_core::EncodingKind::FsstLike
        );
        assert_eq!(
            map_known_vortex_encoding_name("fastlanes_like"),
            shardloom_core::EncodingKind::FastLanesLike
        );
        assert_eq!(
            map_known_vortex_encoding_name("alp_like"),
            shardloom_core::EncodingKind::AlpLike
        );
    }
    #[test]
    fn map_encoding_fsst_fast_lanes_alp_names() {
        assert_eq!(
            map_known_vortex_encoding_name("fsst"),
            shardloom_core::EncodingKind::FsstLike
        );
        assert_eq!(
            map_known_vortex_encoding_name("fast_lanes"),
            shardloom_core::EncodingKind::FastLanesLike
        );
        assert_eq!(
            map_known_vortex_encoding_name("alp"),
            shardloom_core::EncodingKind::AlpLike
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
    fn map_layout_struct_list_sparse() {
        assert_eq!(
            map_known_vortex_layout_name("struct"),
            shardloom_core::LayoutKind::Struct
        );
        assert_eq!(
            map_known_vortex_layout_name("list"),
            shardloom_core::LayoutKind::List
        );
        assert_eq!(
            map_known_vortex_layout_name("sparse"),
            shardloom_core::LayoutKind::Sparse
        );
    }
    #[test]
    fn typed_mapping_status_implemented_is_implemented() {
        assert!(VortexTypedMappingStatus::Implemented.is_implemented());
    }

    #[test]
    fn typed_mapping_status_deferred_unclear_is_not_implemented() {
        assert!(!VortexTypedMappingStatus::DeferredApiUnclear.is_implemented());
    }

    #[test]
    fn dtype_report_implemented_io_and_fallback_disabled() {
        let report = VortexDTypeMappingReport::implemented("vortex::DType");
        assert!(!report.actual_io_implemented);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn dtype_report_deferred_has_name_mapping_available() {
        let report = VortexDTypeMappingReport::deferred_api_unclear();
        assert!(report.name_based_mapping_available);
    }

    #[test]
    fn dtype_report_human_text_mentions_io_and_fallback_status() {
        let text = VortexDTypeMappingReport::deferred_api_unclear().to_human_text();
        assert!(text.contains("actual IO implemented: false"));
        assert!(text.contains("fallback execution allowed: false"));
    }
    #[test]
    fn dtype_report_human_text_renders_non_empty_diagnostics() {
        let mut report = VortexDTypeMappingReport::deferred_api_unclear();
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_dtype_mapping",
            "typed API unresolved",
            "continue using name-based mapping",
        ));
        let text = report.to_human_text();
        assert!(text.contains("diagnostics:"));
        assert!(text.contains("typed API unresolved"));
    }

    #[test]
    fn map_layout_unknown() {
        assert_eq!(
            map_known_vortex_layout_name("??"),
            shardloom_core::LayoutKind::Unknown
        );
    }

    #[test]
    fn encoding_mapping_status_implemented_is_implemented() {
        assert!(VortexEncodingMappingStatus::Implemented.is_implemented());
    }
    #[test]
    fn encoding_mapping_status_deferred_unclear_is_not_implemented() {
        assert!(!VortexEncodingMappingStatus::DeferredApiUnclear.is_implemented());
    }
    #[test]
    fn layout_mapping_status_implemented_is_implemented() {
        assert!(VortexLayoutMappingStatus::Implemented.is_implemented());
    }
    #[test]
    fn layout_mapping_status_deferred_unclear_is_not_implemented() {
        assert!(!VortexLayoutMappingStatus::DeferredApiUnclear.is_implemented());
    }
    #[test]
    fn encoding_layout_report_implemented_io_and_fallback_disabled() {
        let report = VortexEncodingLayoutMappingReport::implemented("api::encoding", "api::layout");
        assert!(!report.actual_io_implemented);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn encoding_layout_report_deferred_name_mapping_available() {
        let report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
        assert!(report.name_based_mapping_available);
        assert!(!report.actual_io_implemented);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn encoding_layout_report_human_text_mentions_io_and_fallback_status() {
        let text = VortexEncodingLayoutMappingReport::deferred_api_unclear().to_human_text();
        assert!(text.contains("actual IO implemented: false"));
        assert!(text.contains("fallback execution allowed: false"));
    }
    #[test]
    fn encoding_layout_report_has_errors_is_severity_based() {
        let mut report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
        assert!(!report.has_errors());
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_encoding_layout_mapping",
            "adapter probe failed",
            "keep mapping deferred",
        ));
        assert!(report.has_errors());
    }
}
