//! Upstream Vortex API discovery and adapter mapping skeleton.
//!
//! This module is planning-only: no file IO, no object-store IO, no decode-to-Arrow
//! default path, and no fallback execution engines.

use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, LogicalDType, Result, ShardLoomError,
};

/// Shared report hygiene helpers for `ShardLoom` Vortex adapter reporting.
///
/// Adapter reports must render non-empty diagnostics in human text, implement
/// severity-based `has_errors`, keep fallback execution disabled visible, and
/// remain planning-only (no real IO in these skeleton reports).
fn diagnostics_have_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|diagnostic| matches!(diagnostic.severity.as_str(), "error" | "fatal"))
}

fn append_diagnostics_section(out: &mut String, diagnostics: &[Diagnostic]) {
    if diagnostics.is_empty() {
        out.push_str("\ndiagnostics: none");
        return;
    }
    out.push_str("\ndiagnostics:");
    for diagnostic in diagnostics {
        let _ = write!(out, "\n- {}", diagnostic.to_human_text());
        if let Some(feature) = &diagnostic.feature {
            let _ = write!(out, " feature={feature}");
        }
        if let Some(reason) = &diagnostic.reason {
            let _ = write!(out, " reason={reason}");
        }
        if let Some(next_step) = &diagnostic.suggested_next_step {
            let _ = write!(out, " next_step={next_step}");
        }
    }
}

fn append_fallback_disabled_line(out: &mut String) {
    out.push_str("\nfallback execution allowed: false");
}

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
                    VortexAdapterCapabilityStatus::Planned,
                ),
                (
                    VortexAdapterCapability::MetadataInspection,
                    VortexAdapterCapabilityStatus::BlockedOnApiDiscovery,
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
        diagnostics_have_errors(&self.diagnostics)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::from(
            "Vortex API inventory\nfallback execution: disabled\nactual IO: not implemented",
        );
        for (cap, status) in &self.capabilities {
            let _ = write!(out, "\n- {}: {}", cap.as_str(), status.as_str());
        }
        append_diagnostics_section(&mut out, &self.diagnostics);
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalIoLaneStatus {
    FeatureGatedRuntime,
    FixtureSmokeOnly,
    Blocked,
}

impl VortexLocalIoLaneStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureGatedRuntime => "feature_gated_runtime",
            Self::FixtureSmokeOnly => "fixture_smoke_only",
            Self::Blocked => "blocked",
        }
    }

    #[must_use]
    pub const fn runtime_available(self) -> bool {
        matches!(self, Self::FeatureGatedRuntime | Self::FixtureSmokeOnly)
    }

    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(self, Self::Blocked)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalIoCoverageRow {
    pub lane_id: &'static str,
    pub io_direction: &'static str,
    pub support_status: VortexLocalIoLaneStatus,
    pub user_surface: &'static str,
    pub feature_gate: &'static str,
    pub upstream_api_surface: &'static str,
    pub schema_encoding_scope: &'static str,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_lane_available: bool,
    pub data_read_lane: bool,
    pub data_written_lane: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexLocalIoCoverageRow {
    #[must_use]
    pub const fn local_primitive_scan_reader() -> Self {
        Self {
            lane_id: "local_vortex_primitive_scan_filter_project",
            io_direction: "read",
            support_status: VortexLocalIoLaneStatus::FixtureSmokeOnly,
            user_surface: "vortex-count,vortex-count-where,vortex-filter,vortex-project,vortex-filter-project,vortex-run",
            feature_gate: "vortex-local-primitives",
            upstream_api_surface: "VortexFile::scan,ScanBuilder::with_filter,ScanBuilder::with_projection,ScanBuilder::into_array_iter",
            schema_encoding_scope: "scoped local primitive fixture columns and reader chunks only",
            correctness_refs: "local_primitive_scan_fixture_correctness,source_backed_scan_evidence",
            benchmark_refs: "vortex-count-benchmark.local_fixture_smoke,traditional_analytics.coverage_table",
            execution_certificate_refs: "certificates/cg16/local-vortex-count/execution.json",
            native_io_certificate_refs: "certificates/cg19/local-vortex-count/native-io.json",
            materialization_decode_refs: "native_io_certificate.no_row_read_no_arrow_no_hidden_materialization",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_GENERALIZED_VORTEX_SOURCE_SPLIT_RUNTIME",
            blocker_id: "gar0005a.generalized_local_reader",
            required_future_evidence: "source_split_certificate,field_mask_evidence,predicate_ordering_evidence,split_serialization_evidence,general_schema_encoding_correctness",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "local primitive Vortex scan lanes only; no broad local reader claim",
            runtime_lane_available: true,
            data_read_lane: true,
            data_written_lane: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn native_count_payload_writer() -> Self {
        Self {
            lane_id: "native_count_output_payload_write",
            io_direction: "write",
            support_status: VortexLocalIoLaneStatus::FeatureGatedRuntime,
            user_surface: "vortex-native-count-payload-write",
            feature_gate: "vortex-write",
            upstream_api_surface: "VortexSessionDefault,SingleThreadRuntime,WriteOptionsSessionExt,PrimitiveArray,Validity,buffer",
            schema_encoding_scope: "single one-row u64 CountAll result payload only",
            correctness_refs: "native_count_payload_write_writes_real_vortex_file",
            benchmark_refs: "not_measured_no_performance_claim",
            execution_certificate_refs: "VortexNativeOutputPayloadWriteReport",
            native_io_certificate_refs: "native_vortex_payload_written=true,vortex_file_written=true,upstream_vortex_write_called=true",
            materialization_decode_refs: "one_scalar_count_result_payload_no_query_materialization_claim",
            policy_refs: "fallback_execution_allowed=false,object_store_io=false,manifest_committed=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_GENERALIZED_VORTEX_PAYLOAD_WRITE",
            blocker_id: "gar0005a.generalized_local_writer",
            required_future_evidence: "schema_payload_matrix,encoding_payload_matrix,correctness_fixture,execution_certificate,native_io_certificate,commit_certificate",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "one local native Vortex CountAll payload write only; no generalized writer claim",
            runtime_lane_available: true,
            data_read_lane: false,
            data_written_lane: true,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn broad_local_writer_blocked() -> Self {
        Self {
            lane_id: "general_local_schema_encoding_writer",
            io_direction: "write",
            support_status: VortexLocalIoLaneStatus::Blocked,
            user_surface: "vortex-output-plan,vortex-output-payload-plan",
            feature_gate: "not_enabled",
            upstream_api_surface: "broader Vortex writer API usage blocked outside the count-payload lane",
            schema_encoding_scope: "general schemas, nested types, null-heavy payloads, dictionaries, structs, lists, and arbitrary output batches",
            correctness_refs: "required_before_admission",
            benchmark_refs: "not_measured_no_performance_claim",
            execution_certificate_refs: "required_before_admission",
            native_io_certificate_refs: "required_before_admission",
            materialization_decode_refs: "required_before_admission",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_GENERALIZED_VORTEX_PAYLOAD_WRITE",
            blocker_id: "gar0005a.generalized_local_writer",
            required_future_evidence: "schema_payload_matrix,encoding_payload_matrix,correctness_fixture,materialization_decode_certificate,native_io_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "general local Vortex writer support remains blocked",
            runtime_lane_available: false,
            data_read_lane: false,
            data_written_lane: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn no_external_fallback(self) -> bool {
        !self.object_store_io
            && !self.table_catalog_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalIoCoverageReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub selected_reader_lane: &'static str,
    pub selected_writer_lane: &'static str,
    pub rows: Vec<VortexLocalIoCoverageRow>,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_written: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexLocalIoCoverageReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_local_io_coverage.v1",
            report_id: "gar0005a.local_vortex_io.coverage",
            gar_id: "GAR-0005-A",
            selected_reader_lane: "local_vortex_primitive_scan_filter_project",
            selected_writer_lane: "native_count_output_payload_write",
            rows: vec![
                VortexLocalIoCoverageRow::local_primitive_scan_reader(),
                VortexLocalIoCoverageRow::native_count_payload_writer(),
                VortexLocalIoCoverageRow::broad_local_writer_blocked(),
            ],
            claim_gate_status: "scoped_evidence_only",
            claim_boundary: "local primitive scan lanes and one native count payload writer only; no object-store, broad schema/encoding writer, table/catalog, lakehouse, SQL/DataFrame, or performance claim",
            runtime_execution: false,
            data_read: false,
            data_written: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.lane_id).collect()
    }

    #[must_use]
    pub fn runtime_lane_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.support_status.runtime_available())
            .map(|row| row.lane_id)
            .collect()
    }

    #[must_use]
    pub fn blocked_lane_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.support_status.is_blocked())
            .map(|row| row.lane_id)
            .collect()
    }

    #[must_use]
    pub fn runtime_lane_count(&self) -> usize {
        self.runtime_lane_ids().len()
    }

    #[must_use]
    pub fn blocked_lane_count(&self) -> usize {
        self.blocked_lane_ids().len()
    }

    #[must_use]
    pub fn selected_lanes_classified(&self) -> bool {
        self.rows
            .iter()
            .any(|row| row.lane_id == self.selected_reader_lane && row.runtime_lane_available)
            && self
                .rows
                .iter()
                .any(|row| row.lane_id == self.selected_writer_lane && row.runtime_lane_available)
    }

    #[must_use]
    pub fn no_external_fallback(&self) -> bool {
        !self.object_store_io
            && !self.table_catalog_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .copied()
                .all(VortexLocalIoCoverageRow::no_external_fallback)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "local Vortex IO coverage\nschema_version: {}\nreport: {}\nselected reader lane: {}\nselected writer lane: {}\nruntime lane count: {}\nblocked lane count: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.selected_reader_lane,
            self.selected_writer_lane,
            self.runtime_lane_count(),
            self.blocked_lane_count(),
            self.claim_gate_status,
        )
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
        diagnostics_have_errors(&self.diagnostics)
    }

    /// Renders a human summary for CLI and operator diagnostics.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex typed DType mapping probe\nstatus: {}\nname-based mapping available: {}\nactual IO implemented: {}",
            self.status.as_str(),
            self.name_based_mapping_available,
            self.actual_io_implemented,
        );
        append_fallback_disabled_line(&mut out);
        if let Some(api) = &self.typed_api_name {
            let _ = write!(out, "\ntyped API: {api}");
        }
        append_diagnostics_section(&mut out, &self.diagnostics);
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

/// Typed statistics mapping status at the Vortex adapter boundary in `ShardLoom`.
///
/// This status captures compile-safe adapter readiness only; no Vortex IO occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStatisticsMappingStatus {
    Implemented,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexStatisticsMappingStatus {
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

    /// Returns whether typed statistics mapping is currently implemented.
    #[must_use]
    pub const fn is_implemented(&self) -> bool {
        matches!(self, Self::Implemented)
    }
}

/// Reporting-only summary for the typed Vortex statistics mapping probe.
///
/// This report describes `ShardLoom` adapter-boundary planning capability for
/// `SegmentStats` mapping without file IO, object-store IO, decode-to-Arrow
/// default behavior, or fallback execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexStatisticsMappingReport {
    pub status: VortexStatisticsMappingStatus,
    pub statistics_api_name: Option<String>,
    pub shardloom_segment_stats_available: bool,
    pub actual_io_implemented: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// Metadata-only IO status for `Vortex` adapter probing in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataIoStatus {
    Implemented,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexMetadataIoStatus {
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
    /// Returns whether metadata-only IO is implemented.
    #[must_use]
    pub const fn is_implemented(&self) -> bool {
        matches!(self, Self::Implemented)
    }
}

/// Metadata probe mode for `VortexMetadataProbeReport`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataIoMode {
    ReportOnly,
    LocalFileMetadataOnly,
    Unsupported,
}
impl VortexMetadataIoMode {
    /// Returns a stable machine-readable mode label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalFileMetadataOnly => "local_file_metadata_only",
            Self::Unsupported => "unsupported",
        }
    }
    /// Returns whether the mode performs local file IO.
    #[must_use]
    pub const fn performs_file_io(&self) -> bool {
        matches!(self, Self::LocalFileMetadataOnly)
    }
}

/// Report for `ShardLoom` metadata-only `Vortex` probing.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataProbeReport {
    pub status: VortexMetadataIoStatus,
    pub mode: VortexMetadataIoMode,
    pub api_name: Option<String>,
    pub target_uri: Option<DatasetUri>,
    pub metadata_available: bool,
    pub schema_available: bool,
    pub statistics_available: bool,
    pub encoding_layout_available: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataProbeReport {
    #[must_use]
    pub fn implemented_metadata_only(api_name: impl Into<String>, target_uri: DatasetUri) -> Self {
        Self {
            status: VortexMetadataIoStatus::Implemented,
            mode: VortexMetadataIoMode::LocalFileMetadataOnly,
            api_name: Some(api_name.into()),
            target_uri: Some(target_uri),
            metadata_available: true,
            schema_available: false,
            statistics_available: false,
            encoding_layout_available: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn deferred_api_unclear() -> Self {
        Self {
            status: VortexMetadataIoStatus::DeferredApiUnclear,
            mode: VortexMetadataIoMode::ReportOnly,
            api_name: None,
            target_uri: None,
            metadata_available: false,
            schema_available: false,
            statistics_available: false,
            encoding_layout_available: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_metadata_probe",
                "Metadata-only Vortex API remains deferred until public API clarity is confirmed.",
                Some("Use report-only mode and retry after API discovery updates.".to_string()),
            )],
        }
    }
    #[must_use]
    pub fn deferred_api_unstable() -> Self {
        Self {
            status: VortexMetadataIoStatus::DeferredApiUnstable,
            ..Self::deferred_api_unclear()
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: VortexMetadataIoStatus::Unsupported,
            mode: VortexMetadataIoMode::Unsupported,
            api_name: None,
            target_uri: None,
            metadata_available: false,
            schema_available: false,
            statistics_available: false,
            encoding_layout_available: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Use local .vortex or file://... paths for report-only probing.".to_string()),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        diagnostics_have_errors(&self.diagnostics)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex metadata-only probe\nmetadata IO status: {}\nmode: {}\nmetadata available: {}\nschema available: {}\nstatistics available: {}\nencoding/layout available: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}",
            self.status.as_str(),
            self.mode.as_str(),
            self.metadata_available,
            self.schema_available,
            self.statistics_available,
            self.encoding_layout_available,
            self.data_materialized,
            self.object_store_io,
            self.write_io
        );
        append_fallback_disabled_line(&mut out);
        if let Some(api_name) = &self.api_name {
            let _ = write!(out, "\nAPI name: {api_name}");
        }
        if let Some(target_uri) = &self.target_uri {
            let _ = write!(out, "\ntarget URI: {}", target_uri.as_str());
        }
        append_diagnostics_section(&mut out, &self.diagnostics);
        out
    }
}

/// Probes `Vortex` metadata-only IO behavior for `ShardLoom`.
///
/// This probe is report-only for now and never performs data scan/decode/materialization,
/// object-store IO, writes, or fallback execution.
///
/// # Errors
///
/// Returns `ShardLoomError` when input URI validation fails.
#[allow(clippy::needless_pass_by_value)]
pub fn probe_vortex_metadata_only(uri: DatasetUri) -> Result<VortexMetadataProbeReport> {
    match uri.scheme() {
        shardloom_core::UriScheme::S3
        | shardloom_core::UriScheme::Gcs
        | shardloom_core::UriScheme::Adls => {
            return Ok(VortexMetadataProbeReport::unsupported(
                "vortex_metadata_probe_object_store",
                "Object-store URI is unsupported for metadata-only probe in this phase.",
            ));
        }
        _ => {}
    }
    if !uri.looks_like_vortex() {
        return Ok(VortexMetadataProbeReport::unsupported(
            "vortex_metadata_probe_uri_validation",
            "Dataset URI does not look like native Vortex input.",
        ));
    }
    Ok(VortexMetadataProbeReport::deferred_api_unclear())
}
impl VortexStatisticsMappingReport {
    /// Creates a report for implemented typed statistics mapping against a public API.
    #[must_use]
    pub fn implemented(statistics_api_name: impl Into<String>) -> Self {
        Self {
            status: VortexStatisticsMappingStatus::Implemented,
            statistics_api_name: Some(statistics_api_name.into()),
            shardloom_segment_stats_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }

    /// Creates a report for deferred typed statistics mapping when public API is unclear.
    #[must_use]
    pub fn deferred_api_unclear() -> Self {
        Self {
            status: VortexStatisticsMappingStatus::DeferredApiUnclear,
            statistics_api_name: None,
            shardloom_segment_stats_available: true,
            actual_io_implemented: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }

    /// Creates a report for deferred typed statistics mapping when public API is unstable.
    #[must_use]
    pub fn deferred_api_unstable() -> Self {
        Self {
            status: VortexStatisticsMappingStatus::DeferredApiUnstable,
            ..Self::deferred_api_unclear()
        }
    }

    /// Appends a deterministic diagnostic message.
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Returns whether report diagnostics include any error/fatal severities.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        diagnostics_have_errors(&self.diagnostics)
    }

    /// Renders a human summary for CLI and operator diagnostics.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex typed statistics mapping probe
statistics mapping status: {}
ShardLoom SegmentStats available: {}
actual IO implemented: {}",
            self.status.as_str(),
            self.shardloom_segment_stats_available,
            self.actual_io_implemented,
        );
        append_fallback_disabled_line(&mut out);
        if let Some(api) = &self.statistics_api_name {
            let _ = write!(
                out,
                "
statistics API: {api}"
            );
        }
        append_diagnostics_section(&mut out, &self.diagnostics);
        out
    }
}

/// Returns an unknown `SegmentStats` placeholder for planning-only statistics mapping.
#[must_use]
pub fn empty_vortex_segment_stats_placeholder() -> shardloom_core::SegmentStats {
    shardloom_core::SegmentStats::unknown()
}

/// Returns a row-count-only `SegmentStats` placeholder for planning-only mapping.
#[must_use]
pub fn row_count_stats_placeholder(row_count: u64) -> shardloom_core::SegmentStats {
    shardloom_core::SegmentStats::with_row_count(row_count)
}

/// Returns whether `ShardLoom` can represent statistics mapping plans without IO.
#[must_use]
pub const fn can_map_statistics_without_io() -> bool {
    true
}

/// Returns whether compile-safe typed upstream Vortex statistics mapping is available.
///
/// This remains deferred until stable public non-IO statistics APIs are confirmed.
#[must_use]
pub const fn typed_vortex_statistics_mapping_available() -> bool {
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
        diagnostics_have_errors(&self.diagnostics)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = format!(
            "Vortex typed encoding/layout mapping probe\nencoding mapping status: {}\nlayout mapping status: {}\nname-based mapping available: {}\nactual IO implemented: {}",
            self.encoding_status.as_str(),
            self.layout_status.as_str(),
            self.name_based_mapping_available,
            self.actual_io_implemented
        );
        append_fallback_disabled_line(&mut out);
        if let Some(api) = &self.encoding_api_name {
            let _ = write!(out, "\nencoding API: {api}");
        }
        if let Some(api) = &self.layout_api_name {
            let _ = write!(out, "\nlayout API: {api}");
        }
        append_diagnostics_section(&mut out, &self.diagnostics);
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
    fn capability_report_human_text_renders_non_empty_diagnostics() {
        let mut report = VortexAdapterCapabilityReport::foundation();
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_capability_mapping",
            "capability probe pending",
            "capability unresolved",
        ));
        let text = report.to_human_text();
        assert!(text.contains("capability unresolved"));
        assert!(text.contains("fallback execution: disabled"));
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
    fn local_io_coverage_classifies_reader_and_writer_lanes() {
        let report = VortexLocalIoCoverageReport::current();
        assert_eq!(report.gar_id, "GAR-0005-A");
        assert!(report.selected_lanes_classified());
        assert_eq!(report.runtime_lane_count(), 2);
        assert_eq!(report.blocked_lane_count(), 1);
        assert!(
            report
                .runtime_lane_ids()
                .contains(&"local_vortex_primitive_scan_filter_project")
        );
        assert!(
            report
                .runtime_lane_ids()
                .contains(&"native_count_output_payload_write")
        );
        assert!(
            report
                .blocked_lane_ids()
                .contains(&"general_local_schema_encoding_writer")
        );
        assert!(report.no_external_fallback());
        assert!(!report.runtime_execution);
        assert!(!report.data_read);
        assert!(!report.data_written);
    }

    #[test]
    fn local_io_coverage_preserves_claim_boundary() {
        let report = VortexLocalIoCoverageReport::current();
        let text = report.to_human_text();
        assert!(text.contains("gar0005a.local_vortex_io.coverage"));
        assert!(text.contains("fallback execution: disabled"));
        assert!(
            report
                .claim_boundary
                .contains("no object-store, broad schema/encoding writer")
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
            "typed API probe pending",
            "typed API unresolved",
        ));
        let text = report.to_human_text();
        assert!(text.contains("diagnostics:"));
        assert!(text.contains("typed API unresolved"));
        assert!(text.contains("fallback execution allowed: false"));
    }
    #[test]
    fn dtype_report_has_errors_is_severity_based() {
        let mut report = VortexDTypeMappingReport::deferred_api_unclear();
        assert!(!report.has_errors());
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_dtype_mapping",
            "typed API unresolved",
            "keep typed mapping deferred",
        ));
        assert!(report.has_errors());
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
    #[test]
    fn encoding_layout_report_human_text_renders_non_empty_diagnostics() {
        let mut report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_encoding_layout_mapping",
            "encoding API probe pending",
            "encoding API unresolved",
        ));
        let text = report.to_human_text();
        assert!(text.contains("encoding API unresolved"));
        assert!(text.contains("fallback execution allowed: false"));
    }
    #[test]
    fn statistics_mapping_status_implemented_is_implemented() {
        assert!(VortexStatisticsMappingStatus::Implemented.is_implemented());
    }

    #[test]
    fn statistics_mapping_status_deferred_unclear_is_not_implemented() {
        assert!(!VortexStatisticsMappingStatus::DeferredApiUnclear.is_implemented());
    }

    #[test]
    fn statistics_report_implemented_io_and_fallback_disabled() {
        let report = VortexStatisticsMappingReport::implemented("vortex::statistics::<public_api>");
        assert!(!report.actual_io_implemented);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn statistics_report_deferred_has_segment_stats_available() {
        let report = VortexStatisticsMappingReport::deferred_api_unclear();
        assert!(report.shardloom_segment_stats_available);
    }

    #[test]
    fn statistics_report_human_text_mentions_io_and_fallback_status() {
        let text = VortexStatisticsMappingReport::deferred_api_unclear().to_human_text();
        assert!(text.contains("actual IO implemented: false"));
        assert!(text.contains("fallback execution allowed: false"));
    }

    #[test]
    fn statistics_report_human_text_renders_non_empty_diagnostics() {
        let mut report = VortexStatisticsMappingReport::deferred_api_unclear();
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_statistics_mapping",
            "statistics API probe pending",
            "statistics API unresolved",
        ));
        let text = report.to_human_text();
        assert!(text.contains("statistics API unresolved"));
        assert!(text.contains("diagnostics:"));
    }

    #[test]
    fn statistics_report_has_errors_is_severity_based() {
        let mut report = VortexStatisticsMappingReport::deferred_api_unclear();
        assert!(!report.has_errors());
        report.add_diagnostic(shardloom_core::Diagnostic::configuration_error(
            "vortex_statistics_mapping",
            "statistics mapping unresolved",
            "keep mapping deferred",
        ));
        assert!(report.has_errors());
    }

    #[test]
    fn empty_segment_stats_placeholder_returns_unknown() {
        assert_eq!(
            empty_vortex_segment_stats_placeholder(),
            shardloom_core::SegmentStats::unknown()
        );
    }

    #[test]
    fn row_count_stats_placeholder_sets_row_count() {
        let stats = row_count_stats_placeholder(42);
        assert_eq!(stats.row_count, Some(42));
    }

    #[test]
    fn can_map_statistics_without_io_returns_true() {
        assert!(can_map_statistics_without_io());
    }
    #[test]
    fn metadata_status_implemented_true() {
        assert!(VortexMetadataIoStatus::Implemented.is_implemented());
    }
    #[test]
    fn metadata_status_deferred_false() {
        assert!(!VortexMetadataIoStatus::DeferredApiUnclear.is_implemented());
    }
    #[test]
    fn metadata_mode_local_file_performs_io() {
        assert!(VortexMetadataIoMode::LocalFileMetadataOnly.performs_file_io());
    }
    #[test]
    fn metadata_mode_report_only_no_file_io() {
        assert!(!VortexMetadataIoMode::ReportOnly.performs_file_io());
    }
    #[test]
    fn metadata_deferred_flags_are_false() {
        let report = VortexMetadataProbeReport::deferred_api_unclear();
        assert!(!report.data_materialized);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn metadata_human_text_contains_required_lines() {
        let text = VortexMetadataProbeReport::deferred_api_unclear().to_human_text();
        assert!(text.contains("data materialized: false"));
        assert!(text.contains("object-store IO: false"));
        assert!(text.contains("write IO: false"));
        assert!(text.contains("fallback execution allowed: false"));
        assert!(text.contains("diagnostics:"));
    }
    #[test]
    fn metadata_has_errors_severity_based() {
        let mut report = VortexMetadataProbeReport::deferred_api_unclear();
        assert!(report.has_errors());
        report.diagnostics.clear();
        assert!(!report.has_errors());
    }
    #[test]
    fn probe_rejects_non_vortex_uri() {
        let uri = DatasetUri::new("file://tmp/not-vortex.parquet").expect("uri");
        let report = probe_vortex_metadata_only(uri).expect("report");
        assert_eq!(report.status, VortexMetadataIoStatus::Unsupported);
    }
    #[test]
    fn probe_rejects_object_store_uri() {
        let uri = DatasetUri::new("s3://bucket/path/data.vortex").expect("uri");
        let report = probe_vortex_metadata_only(uri).expect("report");
        assert_eq!(report.status, VortexMetadataIoStatus::Unsupported);
    }
}
