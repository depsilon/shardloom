//! Upstream Vortex API discovery and adapter mapping skeleton.
//!
//! This module is planning-only: no file IO, no object-store IO, no decode-to-Arrow
//! default path, and no fallback execution engines.

use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
    LogicalDType, Result, ShardLoomError,
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
    pub const fn flat_scalar_vortex_ingest_prepared_state_writer() -> Self {
        Self {
            lane_id: "flat_scalar_vortex_ingest_prepared_state_write",
            io_direction: "write",
            support_status: VortexLocalIoLaneStatus::FeatureGatedRuntime,
            user_surface: "vortex-ingest-smoke,ctx.prepare_vortex,ctx.from_rows(...).write_vortex,ctx.sql_values(...).write_vortex",
            feature_gate: "vortex-write",
            upstream_api_surface: "VortexSessionDefault,SingleThreadRuntime,WriteOptionsSessionExt,VortexFile::scan,ArrayStreamExt::read_all",
            schema_encoding_scope: "flat scalar rows and inferable typed nested source-free outputs admitted by vortex_ingest only",
            correctness_refs: "local_flat_scalar_rows_write_and_reopen_vortex_artifact,sql_local_source_smoke_writes_local_vortex_output_with_certificate_fields,generated_source_vortex_output_writes_local_artifact_and_emits_vortex_evidence",
            benchmark_refs: "traditional_analytics.prepare_once_vortex_ingest,local_python_example_replay",
            execution_certificate_refs: "VortexPreparedStateWriteReport,output_native_io_certificate_status=certified_local_vortex_sink",
            native_io_certificate_refs: "native_io_certificate_status=certified,vortex_preparation_spine_native_io_certificate_status=certified_local_vortex_preparation_spine,reopen_row_count_verified",
            materialization_decode_refs: "flat_scalar_source_state_to_vortex_prepared_state; explicit materialization boundary before Vortex write",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false,object_store_io=false,table_catalog_io=false",
            unsupported_diagnostic_code: "vortex_ingest.requires_vortex_write_feature",
            blocker_id: "prod-ready-1a.generalized_local_vortex_schema_writer",
            required_future_evidence: "broad_schema_payload_matrix,encoding_payload_matrix,nested_layout_certificate,statistics_preservation_matrix,commit_certificate",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "feature-gated local flat scalar/typed source-free Vortex ingest only; no generalized schema, encoding, object-store, table/catalog, or performance claim",
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
    pub const fn flat_columnar_vortex_ingest_prepared_state_writer() -> Self {
        Self {
            lane_id: "flat_columnar_vortex_ingest_prepared_state_write",
            io_direction: "write",
            support_status: VortexLocalIoLaneStatus::FeatureGatedRuntime,
            user_surface: "vortex-ingest-smoke over Parquet, Arrow IPC, Avro, and ORC local SourceState",
            feature_gate: "vortex-write,universal-format-io",
            upstream_api_surface: "ArrayRef::from_arrow(RecordBatch),VortexSessionDefault,WriteOptionsSessionExt,VortexFile::scan,ArrayStreamExt::read_all",
            schema_encoding_scope: "flat columnar SourceState batches with projection mask and Vortex array provider handoff",
            correctness_refs: "vortex_ingest_smoke_preserves_columnar_source_state_for_parquet,vortex_ingest_smoke_preserves_columnar_source_state_for_all_structured_formats",
            benchmark_refs: "traditional_analytics.compatibility_roundtrip_structured_formats",
            execution_certificate_refs: "VortexPreparedStateWriteReport,source_columnar_provider_evidence,vortex_array_build_provider_evidence",
            native_io_certificate_refs: "native_io_certificate_status=certified,vortex_preparation_spine_native_io_certificate_status=certified_local_vortex_preparation_spine,reopen_row_count_verified",
            materialization_decode_refs: "columnar_source_state_preserved_to_vortex_array_provider; no scalar row decode for non-empty batches",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false,object_store_io=false,table_catalog_io=false",
            unsupported_diagnostic_code: "vortex_ingest.requires_universal_format_io_feature",
            blocker_id: "prod-ready-1a.generalized_local_vortex_columnar_writer",
            required_future_evidence: "nested_extension_dtype_matrix,statistics_preservation_matrix,layout_fidelity_report,commit_certificate",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "feature-gated flat columnar local Vortex ingest only; no generalized nested/extension dtype, object-store, table/catalog, or performance claim",
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
            selected_writer_lane: "flat_scalar_vortex_ingest_prepared_state_write",
            rows: vec![
                VortexLocalIoCoverageRow::local_primitive_scan_reader(),
                VortexLocalIoCoverageRow::native_count_payload_writer(),
                VortexLocalIoCoverageRow::flat_scalar_vortex_ingest_prepared_state_writer(),
                VortexLocalIoCoverageRow::flat_columnar_vortex_ingest_prepared_state_writer(),
                VortexLocalIoCoverageRow::broad_local_writer_blocked(),
            ],
            claim_gate_status: "scoped_evidence_only",
            claim_boundary: "local primitive scan lanes, feature-gated flat scalar/columnar Vortex ingest writers, and one native count payload writer only; no object-store, generalized schema/encoding writer, table/catalog, lakehouse, SQL/DataFrame, or performance claim",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexNativeWriterCertificationStatus {
    ScopedFeatureGatedRuntime,
    ProviderCandidatePendingEvidence,
    BlockedPendingEvidence,
}

impl VortexNativeWriterCertificationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopedFeatureGatedRuntime => "scoped_feature_gated_runtime",
            Self::ProviderCandidatePendingEvidence => "provider_candidate_pending_evidence",
            Self::BlockedPendingEvidence => "blocked_pending_evidence",
        }
    }

    #[must_use]
    pub const fn runtime_available(self) -> bool {
        matches!(self, Self::ScopedFeatureGatedRuntime)
    }

    #[must_use]
    pub const fn is_provider_candidate(self) -> bool {
        matches!(self, Self::ProviderCandidatePendingEvidence)
    }

    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(self, Self::BlockedPendingEvidence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexNativeWriterSchemaCertificationRow {
    pub row_id: &'static str,
    pub writer_lane_id: &'static str,
    pub status: VortexNativeWriterCertificationStatus,
    pub feature_gate: &'static str,
    pub provider_decision: &'static str,
    pub provider_surface: &'static str,
    pub schema_family: &'static str,
    pub dtype_scope: &'static str,
    pub validity_scope: &'static str,
    pub encoding_scope: &'static str,
    pub metadata_preservation_status: &'static str,
    pub statistics_preservation_status: &'static str,
    pub materialization_boundary: &'static str,
    pub replay_evidence: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub local_write_runtime: bool,
    pub reopen_verified: bool,
    pub metadata_statistics_broadly_certified: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexNativeWriterSchemaCertificationRow {
    #[must_use]
    pub const fn flat_scalar_rows() -> Self {
        Self {
            row_id: "flat_scalar_rows_nullable_primitives",
            writer_lane_id: "flat_scalar_vortex_ingest_prepared_state_write",
            status: VortexNativeWriterCertificationStatus::ScopedFeatureGatedRuntime,
            feature_gate: "vortex-write",
            provider_decision: "implement_shardloom_kernel",
            provider_surface: "shardloom_scalar_rows_to_vortex_struct",
            schema_family: "flat_scalar_rows",
            dtype_scope: "boolean,int64,uint64,float64_finite,utf8,binary,decimal128,date32,timestamp_micros",
            validity_scope: "nullable_and_all_null_columns_admitted_when_dtype_hint_is_known",
            encoding_scope: "upstream_vortex_writer_default_struct_children",
            metadata_preservation_status: "logical_dtype_and_nullability_preserved_physical_layout_writer_default",
            statistics_preservation_status: "reopen_row_count_verified_statistics_writer_default_not_broadly_certified",
            materialization_boundary: "source_state_scalar_rows_materialized_before_vortex_write",
            replay_evidence: "VortexPreparedStateWriteReport.reopen_row_count_verified",
            unsupported_diagnostic_code: "vortex_ingest.unsupported_scalar_family",
            required_future_evidence: "physical_statistics_preservation_matrix,layout_encoding_matrix,source_selection_vector_fidelity",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "scoped flat scalar local Vortex prepared-state writer only; no arbitrary schema, object-store, table/catalog, or performance claim",
            local_write_runtime: true,
            reopen_verified: true,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn typed_complex_scalar_rows() -> Self {
        Self {
            row_id: "typed_complex_scalar_rows_arrow_provider",
            writer_lane_id: "flat_scalar_vortex_ingest_prepared_state_write",
            status: VortexNativeWriterCertificationStatus::ScopedFeatureGatedRuntime,
            feature_gate: "vortex-write,universal-format-io",
            provider_decision: "use_vortex_native_provider",
            provider_surface: "ArrayRef::from_arrow(RecordBatch)",
            schema_family: "typed_source_free_list_struct_rows",
            dtype_scope: "list_and_struct_when_logical_and_arrow_dtype_hints_are_present",
            validity_scope: "source_schema_driven_validity_through_arrow_record_batch_provider",
            encoding_scope: "vortex_from_arrow_record_batch_writer_default",
            metadata_preservation_status: "typed_nested_logical_schema_preserved_through_vortex_provider",
            statistics_preservation_status: "reopen_row_count_verified_nested_statistics_not_broadly_certified",
            materialization_boundary: "materialized_scalar_rows_to_arrow_record_batch_before_vortex_array_provider",
            replay_evidence: "typed_nested_rows_write_via_vortex_arrow_provider",
            unsupported_diagnostic_code: "vortex_ingest.typed_nested_requires_universal_format_io",
            required_future_evidence: "nested_layout_fidelity_matrix,extension_dtype_matrix,statistics_preservation_matrix",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "scoped typed nested source-free Vortex output only; no arbitrary JSON/map/variant execution or generalized nested writer claim",
            local_write_runtime: true,
            reopen_verified: true,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn flat_columnar_source_state() -> Self {
        Self {
            row_id: "flat_columnar_source_state_arrow_provider",
            writer_lane_id: "flat_columnar_vortex_ingest_prepared_state_write",
            status: VortexNativeWriterCertificationStatus::ScopedFeatureGatedRuntime,
            feature_gate: "vortex-write,universal-format-io",
            provider_decision: "use_vortex_native_provider",
            provider_surface: "ArrayRef::from_arrow(RecordBatch)",
            schema_family: "flat_columnar_source_state",
            dtype_scope: "non_null_boolean,int_widths,uint_widths,float_finite,utf8,binary,date32,timestamp_micros",
            validity_scope: "non_null_columnar_arrays_only_nulls_blocked_before_write",
            encoding_scope: "vortex_from_arrow_record_batch_without_scalar_row_decode",
            metadata_preservation_status: "logical_schema_and_projection_mask_preserved_physical_layout_writer_default",
            statistics_preservation_status: "reopen_row_count_verified_source_statistics_loss_report_required",
            materialization_boundary: "columnar_source_state_preserved_to_vortex_array_provider",
            replay_evidence: "vortex_ingest_smoke_preserves_columnar_source_state_for_all_structured_formats",
            unsupported_diagnostic_code: "vortex_ingest.unsupported_columnar_arrow_type",
            required_future_evidence: "nested_extension_dtype_matrix,layout_statistics_fidelity_report,selection_vector_null_fidelity",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "scoped non-null flat columnar local Vortex prepared-state writer only; nullable flat columnar validity is certified in a separate row; no nested/extension dtype production writer claim",
            local_write_runtime: true,
            reopen_verified: true,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dictionary_encoded_utf8_binary_runtime() -> Self {
        Self {
            row_id: "dictionary_encoded_utf8_binary_provider_gate",
            writer_lane_id: "flat_columnar_vortex_ingest_prepared_state_write",
            status: VortexNativeWriterCertificationStatus::ScopedFeatureGatedRuntime,
            feature_gate: "vortex-write,universal-format-io",
            provider_decision: "use_vortex_native_provider",
            provider_surface: "ArrayRef::from_arrow(RecordBatch) with Arrow DictionaryArray utf8/binary columns",
            schema_family: "dictionary_encoded_utf8_binary_columnar_source_state",
            dtype_scope: "dictionary_encoded_utf8_binary_with_int_key_types",
            validity_scope: "dictionary_null_keys_and_repeated_values_preserved_for_flat_columnar_arrays",
            encoding_scope: "arrow_dictionary_array_provider_roundtrip_physical_vortex_encoding_not_broadly_certified",
            metadata_preservation_status: "logical_dictionary_schema_projection_mask_and_nullability_preserved_physical_layout_writer_default",
            statistics_preservation_status: "reopen_row_count_verified_dictionary_values_replayed_statistics_not_broadly_certified",
            materialization_boundary: "columnar_source_state_preserved_to_vortex_array_provider",
            replay_evidence: "local_flat_columnar_dictionary_source_writes_reopens_values",
            unsupported_diagnostic_code: "vortex_ingest.unsupported_columnar_arrow_type",
            required_future_evidence: "layout_statistics_fidelity_report,interleave_encoding_preservation_matrix,performance_benchmark_evidence",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "scoped flat columnar Arrow dictionary utf8/binary local Vortex prepared-state writer only; no generalized dictionary primitive, interleave, nested/extension dtype, object-store, table/catalog, generalized writer, or performance claim",
            local_write_runtime: true,
            reopen_verified: true,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn dictionary_encoded_primitives_provider_candidate() -> Self {
        Self {
            row_id: "dictionary_encoded_primitives_provider_gate",
            writer_lane_id: "dictionary_encoding_writer_provider_gate",
            status: VortexNativeWriterCertificationStatus::ProviderCandidatePendingEvidence,
            feature_gate: "vortex-write,upstream-vortex",
            provider_decision: "use_vortex_native_provider_pending_gate",
            provider_surface: "vortex_dictionary_and_interleave_encoding_surfaces",
            schema_family: "dictionary_encoded_primitives",
            dtype_scope: "dictionary_encoded_low_cardinality_primitives_and_interleave_layouts",
            validity_scope: "requires_dictionary_validity_and_null_key_semantics_matrix",
            encoding_scope: "requires_dictionary_interleave_encoding_preservation_matrix",
            metadata_preservation_status: "candidate_pending_layout_fidelity_report",
            statistics_preservation_status: "candidate_pending_dictionary_statistics_report",
            materialization_boundary: "blocked_before_runtime_write_until_provider_gate_passes",
            replay_evidence: "required_before_admission",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_DICTIONARY_VORTEX_PAYLOAD_WRITE",
            required_future_evidence: "primitive_dictionary_fixture,dictionary_ordering_fixture,interleave_encoding_preservation_matrix,native_io_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "generalized dictionary primitive and interleave-preserving Vortex writer support remains a provider candidate; scoped utf8/binary Arrow dictionary columnar handoff is certified separately",
            local_write_runtime: false,
            reopen_verified: false,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn nullable_columnar_validity_runtime() -> Self {
        Self {
            row_id: "nullable_columnar_validity_provider_gate",
            writer_lane_id: "flat_columnar_vortex_ingest_prepared_state_write",
            status: VortexNativeWriterCertificationStatus::ScopedFeatureGatedRuntime,
            feature_gate: "vortex-write,universal-format-io",
            provider_decision: "use_vortex_native_provider",
            provider_surface: "ArrayRef::from_arrow(RecordBatch) with nullable columnar validity",
            schema_family: "nullable_columnar_source_state",
            dtype_scope: "nullable_boolean_int_uint_float_utf8_binary_date_timestamp_columns",
            validity_scope: "all_valid_all_null_and_mixed_validity_preserved_for_flat_columnar_arrays",
            encoding_scope: "vortex_from_arrow_record_batch_nullable_validity_writer_default",
            metadata_preservation_status: "logical_schema_nullability_and_projection_mask_preserved_physical_layout_writer_default",
            statistics_preservation_status: "reopen_row_count_verified_null_counts_replayed_statistics_not_broadly_certified",
            materialization_boundary: "columnar_source_state_preserved_to_vortex_array_provider",
            replay_evidence: "local_flat_columnar_nullable_source_writes_reopens_validity",
            unsupported_diagnostic_code: "vortex_ingest.nullable_columnar_writer_requires_validity_matrix",
            required_future_evidence: "sparse_validity_matrix,layout_statistics_fidelity_report,selection_vector_null_fidelity,performance_benchmark_evidence",
            claim_gate_status: "scoped_feature_gated_runtime",
            claim_boundary: "scoped nullable flat columnar local Vortex prepared-state writer only; no sparse/nested/extension dtype, object-store, table/catalog, generalized writer, or performance claim",
            local_write_runtime: true,
            reopen_verified: true,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn extension_dtype_provider_candidate() -> Self {
        Self {
            row_id: "extension_dtype_json_wkb_provider_gate",
            writer_lane_id: "extension_dtype_vortex_writer_provider_gate",
            status: VortexNativeWriterCertificationStatus::ProviderCandidatePendingEvidence,
            feature_gate: "vortex-write,universal-format-io,upstream-vortex",
            provider_decision: "wrap_vortex_concept_pending_gate",
            provider_surface: "vortex_json_wkb_extension_arrow_import_export",
            schema_family: "json_wkb_extension_dtype_preservation",
            dtype_scope: "json_extension,wkb_geospatial_extension,extension_metadata_preservation_only",
            validity_scope: "requires_extension_nullability_and_metadata_roundtrip_matrix",
            encoding_scope: "requires_extension_import_export_layout_fidelity_matrix",
            metadata_preservation_status: "candidate_pending_extension_fidelity_report",
            statistics_preservation_status: "statistics_not_claimed_until_extension_report_exists",
            materialization_boundary: "blocked_before_runtime_write_until_extension_fidelity_and_expression_blockers_pass",
            replay_evidence: "required_before_admission",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_EXTENSION_DTYPE_VORTEX_PAYLOAD_WRITE",
            required_future_evidence: "json_extension_fixture,wkb_extension_fixture,translation_report,unsupported_expression_diagnostics,native_io_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "extension dtype writer support is preservation-candidate only; no JSON/geospatial expression or arbitrary extension execution claim",
            local_write_runtime: false,
            reopen_verified: false,
            metadata_statistics_broadly_certified: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn generalized_schema_encoding_writer_blocked() -> Self {
        Self {
            row_id: "generalized_schema_encoding_writer",
            writer_lane_id: "general_local_schema_encoding_writer",
            status: VortexNativeWriterCertificationStatus::BlockedPendingEvidence,
            feature_gate: "not_enabled",
            provider_decision: "blocked_until_vortex_or_shardloom_evidence",
            provider_surface: "broader Vortex writer API usage blocked outside scoped local lanes",
            schema_family: "generalized_schema_encoding",
            dtype_scope: "arbitrary_nested_extension_dictionary_map_variant_device_and_table_shapes",
            validity_scope: "requires_null_validity_and_extension_semantics_matrix",
            encoding_scope: "requires_encoding_layout_statistics_preservation_matrix",
            metadata_preservation_status: "not_certified",
            statistics_preservation_status: "not_certified",
            materialization_boundary: "blocked_before_write",
            replay_evidence: "required_before_admission",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_GENERALIZED_VORTEX_PAYLOAD_WRITE",
            required_future_evidence: "schema_payload_matrix,encoding_payload_matrix,statistics_preservation_matrix,native_io_certificate,commit_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "general local Vortex writer support remains blocked until generalized schema, encoding, statistics, and commit evidence exists",
            local_write_runtime: false,
            reopen_verified: false,
            metadata_statistics_broadly_certified: false,
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
pub struct VortexNativeWriterSchemaCertificationReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<VortexNativeWriterSchemaCertificationRow>,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub broad_schema_encoding_certification_complete: bool,
    pub metadata_statistics_broadly_certified: bool,
    pub local_runtime_claim_allowed: bool,
    pub performance_claim_allowed: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexNativeWriterSchemaCertificationReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_native_writer_schema_certification.v1",
            report_id: "prod-ready-1a.vortex-native-writer-schema-certification",
            rows: vec![
                VortexNativeWriterSchemaCertificationRow::flat_scalar_rows(),
                VortexNativeWriterSchemaCertificationRow::typed_complex_scalar_rows(),
                VortexNativeWriterSchemaCertificationRow::flat_columnar_source_state(),
                VortexNativeWriterSchemaCertificationRow::nullable_columnar_validity_runtime(),
                VortexNativeWriterSchemaCertificationRow::dictionary_encoded_utf8_binary_runtime(),
                VortexNativeWriterSchemaCertificationRow::dictionary_encoded_primitives_provider_candidate(),
                VortexNativeWriterSchemaCertificationRow::extension_dtype_provider_candidate(),
                VortexNativeWriterSchemaCertificationRow::generalized_schema_encoding_writer_blocked(),
            ],
            claim_gate_status: "scoped_evidence_only",
            claim_boundary: "feature-gated local flat scalar, typed complex source-free, flat columnar, nullable flat columnar, and flat dictionary utf8/binary columnar Vortex prepared-state writer evidence only; generalized dictionary/interleave primitive and extension schema families are provider candidates pending evidence; generalized schema/encoding, object-store, table/catalog, lakehouse, and performance claims remain blocked",
            broad_schema_encoding_certification_complete: false,
            metadata_statistics_broadly_certified: false,
            local_runtime_claim_allowed: true,
            performance_claim_allowed: false,
            object_store_io: false,
            table_catalog_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn scoped_runtime_row_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.runtime_available())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn provider_candidate_row_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_provider_candidate())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn blocked_row_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_blocked())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn scoped_runtime_row_count(&self) -> usize {
        self.scoped_runtime_row_ids().len()
    }

    #[must_use]
    pub fn provider_candidate_row_count(&self) -> usize {
        self.provider_candidate_row_ids().len()
    }

    #[must_use]
    pub fn blocked_row_count(&self) -> usize {
        self.blocked_row_ids().len()
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
                .all(VortexNativeWriterSchemaCertificationRow::no_external_fallback)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "Vortex native writer schema certification\nschema_version: {}\nreport: {}\nscoped runtime rows: {}\nprovider candidate rows: {}\nblocked rows: {}\nbroad schema certification complete: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scoped_runtime_row_count(),
            self.provider_candidate_row_count(),
            self.blocked_row_count(),
            self.broad_schema_encoding_certification_complete,
            self.claim_gate_status,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vortex075HeavyOperatorSurface {
    GroupedSumCountAggregate,
    ValidityMaskNoNull,
    BranchlessZip,
    DictionaryFsstReuse,
    LayoutChildCache,
    ByteLengthExpression,
    DataFusion54Integration,
}

impl Vortex075HeavyOperatorSurface {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GroupedSumCountAggregate => "grouped_sum_count_aggregate",
            Self::ValidityMaskNoNull => "validity_mask_no_null",
            Self::BranchlessZip => "branchless_zip",
            Self::DictionaryFsstReuse => "dictionary_fsst_reuse",
            Self::LayoutChildCache => "layout_child_cache",
            Self::ByteLengthExpression => "byte_length_expression",
            Self::DataFusion54Integration => "datafusion_54_integration",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vortex075HeavyOperatorDispositionStatus {
    CandidatePendingProviderGate,
    WrappedByExistingShardLoomKernel,
    BlockedExternalIntegration,
}

impl Vortex075HeavyOperatorDispositionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidatePendingProviderGate => "candidate_pending_provider_gate",
            Self::WrappedByExistingShardLoomKernel => "wrapped_by_existing_shardloom_kernel",
            Self::BlockedExternalIntegration => "blocked_external_integration",
        }
    }

    #[must_use]
    pub const fn is_candidate(self) -> bool {
        matches!(self, Self::CandidatePendingProviderGate)
    }

    #[must_use]
    pub const fn is_wrapped(self) -> bool {
        matches!(self, Self::WrappedByExistingShardLoomKernel)
    }

    #[must_use]
    pub const fn is_blocked_external(self) -> bool {
        matches!(self, Self::BlockedExternalIntegration)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Vortex075HeavyOperatorDispositionRow {
    pub surface: Vortex075HeavyOperatorSurface,
    pub status: Vortex075HeavyOperatorDispositionStatus,
    pub operator_family: &'static str,
    pub upstream_api_surface: &'static str,
    pub shardloom_disposition: &'static str,
    pub required_evidence: &'static str,
    pub provider_gate_required: bool,
    pub decoded_reference_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub claim_gate_status: &'static str,
}

impl Vortex075HeavyOperatorDispositionRow {
    #[must_use]
    pub const fn candidate(
        surface: Vortex075HeavyOperatorSurface,
        operator_family: &'static str,
        upstream_api_surface: &'static str,
        shardloom_disposition: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: Vortex075HeavyOperatorDispositionStatus::CandidatePendingProviderGate,
            operator_family,
            upstream_api_surface,
            shardloom_disposition,
            required_evidence,
            provider_gate_required: true,
            decoded_reference_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn wrapped(
        surface: Vortex075HeavyOperatorSurface,
        operator_family: &'static str,
        upstream_api_surface: &'static str,
        shardloom_disposition: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: Vortex075HeavyOperatorDispositionStatus::WrappedByExistingShardLoomKernel,
            operator_family,
            upstream_api_surface,
            shardloom_disposition,
            required_evidence,
            provider_gate_required: true,
            decoded_reference_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn blocked_external(
        surface: Vortex075HeavyOperatorSurface,
        operator_family: &'static str,
        upstream_api_surface: &'static str,
        shardloom_disposition: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: Vortex075HeavyOperatorDispositionStatus::BlockedExternalIntegration,
            operator_family,
            upstream_api_surface,
            shardloom_disposition,
            required_evidence,
            provider_gate_required: false,
            decoded_reference_required: false,
            execution_certificate_required: false,
            native_io_certificate_required: false,
            benchmark_evidence_required: false,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(self) -> bool {
        !self.runtime_execution_allowed
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Option<Diagnostic> {
        if !self.status.is_blocked_external() {
            return None;
        }
        Some(Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Info,
            DiagnosticCategory::NoFallbackPolicy,
            format!(
                "{} is external-integration-only and cannot execute ShardLoom work",
                self.surface.as_str()
            ),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} remains {}.",
                self.upstream_api_surface, self.shardloom_disposition
            )),
            Some(
                "Keep Vortex query-engine integrations as baselines or oracles only; use ShardLoom-native provider gates for runtime work."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Vortex075HeavyOperatorProviderDispositionReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub phase_id: &'static str,
    pub upstream_vortex_provider_version: &'static str,
    pub gate_status: &'static str,
    pub support_status: &'static str,
    pub rows: Vec<Vortex075HeavyOperatorDispositionRow>,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl Vortex075HeavyOperatorProviderDispositionReport {
    #[must_use]
    pub fn current() -> Self {
        let rows = vortex075_heavy_operator_disposition_rows();
        let diagnostics = rows
            .iter()
            .copied()
            .filter_map(Vortex075HeavyOperatorDispositionRow::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.vortex075_heavy_operator_provider_disposition.v1",
            report_id: "perf-runtime-7b.vortex075.heavy_operator_provider_disposition",
            phase_id: "PERF-RUNTIME-7B",
            upstream_vortex_provider_version: crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
            gate_status: "report_only",
            support_status: "provider_disposition_recorded",
            rows,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Vortex 0.75 heavy-operator surfaces are mapped to provider candidates, existing ShardLoom kernels, or blocked external integrations; no new runtime admission or performance claim is made.",
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.surface.as_str()).collect()
    }

    #[must_use]
    pub fn provider_candidate_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_candidate())
            .count()
    }

    #[must_use]
    pub fn wrapped_shardloom_kernel_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_wrapped())
            .count()
    }

    #[must_use]
    pub fn blocked_external_integration_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_blocked_external())
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .rows
                .iter()
                .copied()
                .all(Vortex075HeavyOperatorDispositionRow::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.claim_gate_status != "not_claim_grade"
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
            "Vortex 0.75 heavy-operator provider disposition\nschema_version: {}\nreport: {}\nprovider version: {}\nprovider candidates: {}\nwrapped ShardLoom kernels: {}\nblocked external integrations: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.upstream_vortex_provider_version,
            self.provider_candidate_count(),
            self.wrapped_shardloom_kernel_count(),
            self.blocked_external_integration_count(),
            self.claim_gate_status,
        )
    }
}

fn vortex075_heavy_operator_disposition_rows() -> Vec<Vortex075HeavyOperatorDispositionRow> {
    use Vortex075HeavyOperatorSurface as S;
    vec![
        Vortex075HeavyOperatorDispositionRow::candidate(
            S::GroupedSumCountAggregate,
            "grouped_aggregates",
            "vortex_0_75_grouped_sum_count_kernels",
            "evaluate_before_adding_another_shardloom_owned_group_state_kernel",
            "provider_gate,decoded_reference_parity,null_key_semantics,execution_certificate,native_io_certificate,claim_grade_benchmark_row",
        ),
        Vortex075HeavyOperatorDispositionRow::candidate(
            S::ValidityMaskNoNull,
            "null_heavy_aggregate",
            "vortex_0_75_validity_mask_execute_no_nulls",
            "candidate_for_no_null_and_null_heavy_operator_admission",
            "provider_gate,null_semantics_parity,mask_alltrue_allfalse_fixtures,execution_certificate,benchmark_row",
        ),
        Vortex075HeavyOperatorDispositionRow::candidate(
            S::BranchlessZip,
            "filter_project_fusion",
            "vortex_0_75_branchless_primitive_boolean_zip",
            "candidate_for_selection_vector_and_filter_project_hot_paths",
            "provider_gate,selection_vector_parity,boolean_null_semantics,microbenchmark,route_benchmark_row",
        ),
        Vortex075HeavyOperatorDispositionRow::candidate(
            S::DictionaryFsstReuse,
            "dictionary_group_key",
            "vortex_0_75_dictionary_slice_fsst_state_sharing",
            "candidate_for_string_group_key_and_dictionary_reuse_before_broad_grouped_claim",
            "provider_gate,dictionary_ordering_fixture,utf8_null_fixture,decoded_reference_parity,benchmark_row",
        ),
        Vortex075HeavyOperatorDispositionRow::candidate(
            S::LayoutChildCache,
            "reader_input_cache",
            "vortex_0_75_layout_child_cache",
            "candidate_input_cache_for_prepared_operator_chunks_before_local_cache_duplication",
            "provider_gate,source_fingerprint_validation,cache_scope_evidence,no_decode_no_materialization_evidence,native_io_certificate",
        ),
        Vortex075HeavyOperatorDispositionRow::wrapped(
            S::ByteLengthExpression,
            "binary_string_expression",
            "vortex_0_75_byte_length_expression",
            "existing_shardloom_binary_byte_length_kernel_remains_selected_until_provider_parity_is_proven",
            "provider_gate,string_binary_null_parity,decoded_reference_parity,execution_certificate,benchmark_row",
        ),
        Vortex075HeavyOperatorDispositionRow::blocked_external(
            S::DataFusion54Integration,
            "external_baseline_only",
            "vortex_0_75_datafusion_54_integration",
            "baseline_or_oracle_only_not_shardloom_runtime_provider",
            "none_for_runtime_admission; external-baseline policy only",
        ),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vortex075LocalIoSurface {
    LayoutReaderContextCache,
    JsonExtensionArrowInterop,
    WkbGeospatialExtension,
    InterleaveEncoding,
    BinaryZstdCompression,
    RowByteEncoder,
    ValidityMaskSemantics,
    ArrowDeviceGpuPath,
}

impl Vortex075LocalIoSurface {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LayoutReaderContextCache => "layout_reader_context_cache",
            Self::JsonExtensionArrowInterop => "json_extension_arrow_interop",
            Self::WkbGeospatialExtension => "wkb_geospatial_extension",
            Self::InterleaveEncoding => "interleave_encoding",
            Self::BinaryZstdCompression => "binary_zstd_compression",
            Self::RowByteEncoder => "row_byte_encoder",
            Self::ValidityMaskSemantics => "validity_mask_semantics",
            Self::ArrowDeviceGpuPath => "arrow_device_gpu_path",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vortex075LocalIoDispositionStatus {
    CandidatePendingProviderGate,
    BlockedFutureDeviceTrack,
}

impl Vortex075LocalIoDispositionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidatePendingProviderGate => "candidate_pending_provider_gate",
            Self::BlockedFutureDeviceTrack => "blocked_future_device_track",
        }
    }

    #[must_use]
    pub const fn is_candidate(self) -> bool {
        matches!(self, Self::CandidatePendingProviderGate)
    }

    #[must_use]
    pub const fn is_blocked_future_device(self) -> bool {
        matches!(self, Self::BlockedFutureDeviceTrack)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Vortex075LocalIoDispositionRow {
    pub surface: Vortex075LocalIoSurface,
    pub status: Vortex075LocalIoDispositionStatus,
    pub format_family: &'static str,
    pub upstream_api_surface: &'static str,
    pub shardloom_disposition: &'static str,
    pub required_evidence: &'static str,
    pub provider_gate_required: bool,
    pub fidelity_report_required: bool,
    pub deterministic_blocker_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub runtime_execution_allowed: bool,
    pub data_read_allowed: bool,
    pub data_written_allowed: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub claim_gate_status: &'static str,
}

impl Vortex075LocalIoDispositionRow {
    #[must_use]
    pub const fn candidate(
        surface: Vortex075LocalIoSurface,
        format_family: &'static str,
        upstream_api_surface: &'static str,
        shardloom_disposition: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: Vortex075LocalIoDispositionStatus::CandidatePendingProviderGate,
            format_family,
            upstream_api_surface,
            shardloom_disposition,
            required_evidence,
            provider_gate_required: true,
            fidelity_report_required: true,
            deterministic_blocker_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            runtime_execution_allowed: false,
            data_read_allowed: false,
            data_written_allowed: false,
            data_decoded: false,
            data_materialized: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn blocked_future_device(
        surface: Vortex075LocalIoSurface,
        format_family: &'static str,
        upstream_api_surface: &'static str,
        shardloom_disposition: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: Vortex075LocalIoDispositionStatus::BlockedFutureDeviceTrack,
            format_family,
            upstream_api_surface,
            shardloom_disposition,
            required_evidence,
            provider_gate_required: false,
            fidelity_report_required: true,
            deterministic_blocker_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            runtime_execution_allowed: false,
            data_read_allowed: false,
            data_written_allowed: false,
            data_decoded: false,
            data_materialized: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(self) -> bool {
        !self.runtime_execution_allowed
            && !self.data_read_allowed
            && !self.data_written_allowed
            && !self.data_decoded
            && !self.data_materialized
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Option<Diagnostic> {
        if !self.status.is_blocked_future_device() {
            return None;
        }
        Some(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Info,
            DiagnosticCategory::VortexIo,
            format!(
                "{} is blocked until the device-residency track is certified",
                self.surface.as_str()
            ),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} remains {}.",
                self.upstream_api_surface, self.shardloom_disposition
            )),
            Some(
                "Keep Arrow device/GPU/JNI/cuDF paths out of local v1 runtime claims until device memory ownership, packaging, certificates, and no-fallback evidence are attached."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Vortex075LocalIoProviderDispositionReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub phase_id: &'static str,
    pub upstream_vortex_provider_version: &'static str,
    pub gate_status: &'static str,
    pub support_status: &'static str,
    pub rows: Vec<Vortex075LocalIoDispositionRow>,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_written: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl Vortex075LocalIoProviderDispositionReport {
    #[must_use]
    pub fn current() -> Self {
        let rows = vortex075_local_io_disposition_rows();
        let diagnostics = rows
            .iter()
            .copied()
            .filter_map(Vortex075LocalIoDispositionRow::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.vortex075_local_io_provider_disposition.v1",
            report_id: "prod-ready-1a.vortex075.local_io_provider_disposition",
            phase_id: "PROD-READY-1A",
            upstream_vortex_provider_version: crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
            gate_status: "report_only",
            support_status: "provider_disposition_recorded",
            rows,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Vortex 0.75 local-I/O surfaces are mapped to provider candidates or blocked future/device tracks; no local-format production claim, runtime admission, decode, read, write, or performance claim is made.",
            runtime_execution: false,
            data_read: false,
            data_written: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            table_catalog_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.surface.as_str()).collect()
    }

    #[must_use]
    pub fn provider_candidate_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_candidate())
            .count()
    }

    #[must_use]
    pub fn blocked_future_device_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_blocked_future_device())
            .count()
    }

    #[must_use]
    pub fn deterministic_blocker_required_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.deterministic_blocker_required)
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.runtime_execution
            && !self.data_read
            && !self.data_written
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.table_catalog_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .rows
                .iter()
                .copied()
                .all(Vortex075LocalIoDispositionRow::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.claim_gate_status != "not_claim_grade"
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
            "Vortex 0.75 local-I/O provider disposition\nschema_version: {}\nreport: {}\nprovider version: {}\nprovider candidates: {}\nblocked future/device tracks: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.upstream_vortex_provider_version,
            self.provider_candidate_count(),
            self.blocked_future_device_count(),
            self.claim_gate_status,
        )
    }
}

fn vortex075_local_io_disposition_rows() -> Vec<Vortex075LocalIoDispositionRow> {
    use Vortex075LocalIoSurface as S;
    vec![
        Vortex075LocalIoDispositionRow::candidate(
            S::LayoutReaderContextCache,
            "vortex_native_local_read",
            "vortex_0_75_layout_reader_context_child_cache",
            "candidate_for_prepared_read_through_cache_before_shardloom_local_cache_duplication",
            "provider_gate,source_fingerprint_validation,cache_scope_evidence,no_decode_no_materialization_evidence,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::JsonExtensionArrowInterop,
            "json_extension",
            "vortex_0_75_json_extension_arrow_import_export",
            "candidate_for_json_extension_preservation_and_deterministic_expression_blockers",
            "provider_gate,json_extension_fidelity_report,arrow_boundary_report,unsupported_expression_diagnostic,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::WkbGeospatialExtension,
            "geospatial_extension",
            "vortex_0_75_wkb_geo_extension_arrow_interop",
            "candidate_for_metadata_preservation_only_with_execution_blockers",
            "provider_gate,wkb_extension_fidelity_report,geo_execution_blocker,translation_report,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::InterleaveEncoding,
            "encoded_layout",
            "vortex_0_75_interleave_encoding",
            "candidate_for_encoding_preservation_and_layout_fidelity_report",
            "provider_gate,encoding_preservation_fixture,layout_fidelity_report,statistics_preservation_report,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::BinaryZstdCompression,
            "binary_compression",
            "vortex_0_75_binary_zstd_compression",
            "candidate_for_compression_metadata_and_write_fidelity_report",
            "provider_gate,compression_metadata_fixture,write_fidelity_report,read_replay_certificate,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::RowByteEncoder,
            "local_write_path",
            "vortex_0_75_row_oriented_byte_encoder",
            "candidate_for_write_path_evaluation_before_general_writer_claim",
            "provider_gate,row_encoder_semantic_fixture,write_materialization_boundary,native_io_certificate,benchmark_row",
        ),
        Vortex075LocalIoDispositionRow::candidate(
            S::ValidityMaskSemantics,
            "null_validity",
            "vortex_0_75_validity_mask_semantics",
            "candidate_for_adapter_null_semantics_before_broad_format_claim",
            "provider_gate,alltrue_allfalse_mask_fixture,null_roundtrip_fixture,decoded_reference_parity,native_io_certificate",
        ),
        Vortex075LocalIoDispositionRow::blocked_future_device(
            S::ArrowDeviceGpuPath,
            "device_acceleration",
            "vortex_0_75_arrow_device_gpu_cudf_jni_paths",
            "blocked_future_device_track_not_local_cpu_v1",
            "device_residency_policy,package_build_policy,cpu_fallback_refusal,execution_certificate,native_io_certificate,benchmark_row",
        ),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexObjectStoreIoGateSurface {
    ObjectStoreVortexReadProvider,
    ObjectStoreVortexWriteProvider,
    CredentialPolicy,
    RangeRequestBudget,
    WriteIdempotency,
    UpstreamSinkApi,
    NativeIoCertificate,
    UnsupportedDiagnostic,
}

impl VortexObjectStoreIoGateSurface {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectStoreVortexReadProvider => "object_store_vortex_read_provider",
            Self::ObjectStoreVortexWriteProvider => "object_store_vortex_write_provider",
            Self::CredentialPolicy => "credential_policy",
            Self::RangeRequestBudget => "range_request_budget",
            Self::WriteIdempotency => "write_idempotency",
            Self::UpstreamSinkApi => "upstream_sink_api",
            Self::NativeIoCertificate => "native_io_certificate",
            Self::UnsupportedDiagnostic => "unsupported_diagnostic",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreVortexReadProvider | Self::ObjectStoreVortexWriteProvider => {
                DiagnosticCode::ObjectStoreUnsupported
            }
            Self::CredentialPolicy => DiagnosticCode::ExternalEffectDisabled,
            Self::RangeRequestBudget => DiagnosticCode::ResourceBudgetExceeded,
            Self::WriteIdempotency => DiagnosticCode::CommitNotAtomic,
            Self::UpstreamSinkApi | Self::NativeIoCertificate => DiagnosticCode::NotImplemented,
            Self::UnsupportedDiagnostic => DiagnosticCode::NoFallbackExecution,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreVortexReadProvider | Self::ObjectStoreVortexWriteProvider => {
                DiagnosticCategory::ObjectStore
            }
            Self::CredentialPolicy => DiagnosticCategory::ExternalEffect,
            Self::RangeRequestBudget => DiagnosticCategory::ResourceBudget,
            Self::WriteIdempotency => DiagnosticCategory::Execution,
            Self::UpstreamSinkApi | Self::NativeIoCertificate => DiagnosticCategory::VortexIo,
            Self::UnsupportedDiagnostic => DiagnosticCategory::NoFallbackPolicy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexObjectStoreIoGateStatus {
    ReportOnlyAvailable,
    UnsupportedUntilCertified,
}

impl VortexObjectStoreIoGateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnlyAvailable => "report_only_available",
            Self::UnsupportedUntilCertified => "unsupported_until_certified",
        }
    }

    #[must_use]
    pub const fn is_report_only(self) -> bool {
        matches!(self, Self::ReportOnlyAvailable)
    }

    #[must_use]
    pub const fn is_unsupported(self) -> bool {
        matches!(self, Self::UnsupportedUntilCertified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexObjectStoreIoGateRow {
    pub surface: VortexObjectStoreIoGateSurface,
    pub status: VortexObjectStoreIoGateStatus,
    pub user_surface: &'static str,
    pub upstream_api_surface: &'static str,
    pub required_evidence: &'static str,
    pub provider_requirement: bool,
    pub credential_requirement: bool,
    pub idempotency_requirement: bool,
    pub upstream_api_requirement: bool,
    pub runtime_execution_allowed: bool,
    pub object_store_io_allowed: bool,
    pub write_io_allowed: bool,
    pub data_read_allowed: bool,
    pub data_written_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub claim_gate_status: &'static str,
}

impl VortexObjectStoreIoGateRow {
    #[must_use]
    pub const fn unsupported(
        surface: VortexObjectStoreIoGateSurface,
        user_surface: &'static str,
        upstream_api_surface: &'static str,
        required_evidence: &'static str,
        requirements: VortexObjectStoreIoRequirements,
    ) -> Self {
        Self {
            surface,
            status: VortexObjectStoreIoGateStatus::UnsupportedUntilCertified,
            user_surface,
            upstream_api_surface,
            required_evidence,
            provider_requirement: requirements.provider,
            credential_requirement: requirements.credential,
            idempotency_requirement: requirements.idempotency,
            upstream_api_requirement: requirements.upstream_api,
            runtime_execution_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            data_read_allowed: false,
            data_written_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn report_only(
        surface: VortexObjectStoreIoGateSurface,
        user_surface: &'static str,
        upstream_api_surface: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: VortexObjectStoreIoGateStatus::ReportOnlyAvailable,
            user_surface,
            upstream_api_surface,
            required_evidence,
            provider_requirement: false,
            credential_requirement: false,
            idempotency_requirement: false,
            upstream_api_requirement: false,
            runtime_execution_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            data_read_allowed: false,
            data_written_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(self) -> bool {
        !self.runtime_execution_allowed
            && !self.object_store_io_allowed
            && !self.write_io_allowed
            && !self.data_read_allowed
            && !self.data_written_allowed
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Option<Diagnostic> {
        if !self.status.is_unsupported() {
            return None;
        }
        Some(Diagnostic::new(
            self.surface.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.surface.diagnostic_category(),
            format!("{} is unsupported until certified", self.surface.as_str()),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} requires {} before object-store Vortex I/O admission.",
                self.surface.as_str(),
                self.required_evidence
            )),
            Some(
                "Keep the object-store Vortex lane report-only until provider, credential, idempotency, upstream API, and Native I/O evidence are attached."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexObjectStoreIoRequirements {
    pub provider: bool,
    pub credential: bool,
    pub idempotency: bool,
    pub upstream_api: bool,
}

impl VortexObjectStoreIoRequirements {
    pub const PROVIDER_CREDENTIAL_UPSTREAM: Self = Self {
        provider: true,
        credential: true,
        idempotency: false,
        upstream_api: true,
    };
    pub const WRITE_PROVIDER_CREDENTIAL_IDEMPOTENCY_UPSTREAM: Self = Self {
        provider: true,
        credential: true,
        idempotency: true,
        upstream_api: true,
    };
    pub const CREDENTIAL_ONLY: Self = Self {
        provider: false,
        credential: true,
        idempotency: false,
        upstream_api: false,
    };
    pub const PROVIDER_ONLY: Self = Self {
        provider: true,
        credential: false,
        idempotency: false,
        upstream_api: false,
    };
    pub const IDEMPOTENCY_ONLY: Self = Self {
        provider: false,
        credential: false,
        idempotency: true,
        upstream_api: false,
    };
    pub const UPSTREAM_ONLY: Self = Self {
        provider: false,
        credential: false,
        idempotency: false,
        upstream_api: true,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexObjectStoreIoGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub gate_status: &'static str,
    pub support_status: &'static str,
    pub rows: Vec<VortexObjectStoreIoGateRow>,
    pub required_policy_refs: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub object_store_read_execution_allowed: bool,
    pub object_store_write_execution_allowed: bool,
    pub upstream_vortex_read_allowed: bool,
    pub upstream_vortex_write_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub credentials_resolved: bool,
    pub provider_probe: bool,
    pub network_probe: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_written: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub provider_capability_policy_required: bool,
    pub credential_policy_required: bool,
    pub range_request_budget_required: bool,
    pub idempotency_key_required: bool,
    pub upstream_api_reference_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexObjectStoreIoGateReport {
    #[must_use]
    pub fn current() -> Self {
        let rows = vortex_object_store_io_gate_rows();
        let diagnostics = rows
            .iter()
            .copied()
            .filter_map(VortexObjectStoreIoGateRow::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.vortex_object_store_io_gate.v1",
            report_id: "gar0005b.vortex_object_store_io.gate",
            gar_id: "GAR-0005-B",
            gate_status: "report_only",
            support_status: "unsupported",
            rows,
            required_policy_refs: "provider_capability_policy,credential_effect_policy,range_request_budget,idempotency_key_contract,upstream_api_reference,execution_certificate,native_io_certificate,benchmark_evidence,no_fallback_policy",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "object-store Vortex read/write providers and upstream write integration are report-only/unsupported; no object-store I/O, credential resolution, network probe, upstream write, table/catalog, lakehouse, SQL/DataFrame, or performance claim",
            object_store_read_execution_allowed: false,
            object_store_write_execution_allowed: false,
            upstream_vortex_read_allowed: false,
            upstream_vortex_write_allowed: false,
            credential_resolution_allowed: false,
            credentials_resolved: false,
            provider_probe: false,
            network_probe: false,
            runtime_execution: false,
            data_read: false,
            data_written: false,
            object_store_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            provider_capability_policy_required: true,
            credential_policy_required: true,
            range_request_budget_required: true,
            idempotency_key_required: true,
            upstream_api_reference_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            diagnostics,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.surface.as_str()).collect()
    }

    #[must_use]
    pub fn unsupported_surface_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .count()
    }

    #[must_use]
    pub fn report_only_surface_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_report_only())
            .count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .filter(|row| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == row.surface.diagnostic_code()
                        && diagnostic.category == row.surface.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(row.surface.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        self.unsupported_surface_count() > 0
            && self.unsupported_diagnostic_count() == self.unsupported_surface_count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .map(|row| row.surface.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.object_store_read_execution_allowed
            && !self.object_store_write_execution_allowed
            && !self.upstream_vortex_read_allowed
            && !self.upstream_vortex_write_allowed
            && !self.credential_resolution_allowed
            && !self.credentials_resolved
            && !self.provider_probe
            && !self.network_probe
            && !self.runtime_execution
            && !self.data_read
            && !self.data_written
            && !self.object_store_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .rows
                .iter()
                .copied()
                .all(VortexObjectStoreIoGateRow::side_effect_free)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || !self.deterministic_unsupported_diagnostics_ready()
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
            "object-store Vortex I/O gate\nschema_version: {}\nreport: {}\ngate: {}\nsupport: {}\nunsupported surfaces: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.gate_status,
            self.support_status,
            self.unsupported_surface_count(),
            self.claim_gate_status,
        )
    }
}

fn vortex_object_store_io_gate_rows() -> Vec<VortexObjectStoreIoGateRow> {
    use VortexObjectStoreIoGateSurface as S;
    use VortexObjectStoreIoRequirements as R;
    vec![
        VortexObjectStoreIoGateRow::unsupported(
            S::ObjectStoreVortexReadProvider,
            "vortex-api-inventory,object-store-request-plan,cg10-object-store-runtime-gate",
            "Vortex object-store source/provider read path",
            "provider_capability_policy,credential_effect_policy,range_request_budget,native_io_certificate",
            R::PROVIDER_CREDENTIAL_UPSTREAM,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::ObjectStoreVortexWriteProvider,
            "vortex-api-inventory,object-store-commit-plan",
            "Vortex object-store sink/provider write path",
            "provider_capability_policy,credential_effect_policy,idempotency_key_contract,upstream_write_api_ref,native_io_certificate,commit_certificate",
            R::WRITE_PROVIDER_CREDENTIAL_IDEMPOTENCY_UPSTREAM,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::CredentialPolicy,
            "security-plan,effect-budget-plan,object-store-request-plan",
            "credential provider and secret materialization boundary",
            "explicit_secret_source,redaction_policy,least_privilege_scope,effect_budget",
            R::CREDENTIAL_ONLY,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::RangeRequestBudget,
            "object-store-range-plan,object-store-coalesce-plan,object-store-schedule-plan",
            "bounded object-store byte-range request planner",
            "byte_range_provider_gate,request_budget_policy,retry_policy,benchmark_object_store_request_metric",
            R::PROVIDER_ONLY,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::WriteIdempotency,
            "object-store-commit-plan,vortex-local-commit-execute",
            "object-store write idempotency and commit recovery contract",
            "idempotency_key_contract,commit_protocol,recovery_certificate",
            R::IDEMPOTENCY_ONLY,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::UpstreamSinkApi,
            "vortex-api-inventory,vortex-output-payload-plan",
            "upstream Vortex sink/write API for object-store targets",
            "upstream_vortex_sink_api_ref,write_options_policy,schema_encoding_matrix",
            R::UPSTREAM_ONLY,
        ),
        VortexObjectStoreIoGateRow::unsupported(
            S::NativeIoCertificate,
            "native-io-envelope-plan,execution-certificate-plan",
            "object-store Vortex Native I/O certificate",
            "execution_certificate,native_io_certificate,no_decode_materialization_policy,no_fallback_policy",
            R::PROVIDER_CREDENTIAL_UPSTREAM,
        ),
        VortexObjectStoreIoGateRow::report_only(
            S::UnsupportedDiagnostic,
            "vortex-api-inventory,object-store-request-plan",
            "stable unsupported diagnostics and claim boundary",
            "stable_diagnostic_code,support_status,claim_gate_status,no_fallback_policy",
        ),
    ]
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
        assert_eq!(report.runtime_lane_count(), 4);
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
                .runtime_lane_ids()
                .contains(&"flat_scalar_vortex_ingest_prepared_state_write")
        );
        assert!(
            report
                .runtime_lane_ids()
                .contains(&"flat_columnar_vortex_ingest_prepared_state_write")
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
                .contains("no object-store, generalized schema/encoding writer")
        );
    }

    #[test]
    fn native_writer_schema_certification_classifies_scoped_and_blocked_rows() {
        let report = VortexNativeWriterSchemaCertificationReport::current();

        assert_eq!(
            report.schema_version,
            "shardloom.vortex_native_writer_schema_certification.v1"
        );
        assert_eq!(
            report.report_id,
            "prod-ready-1a.vortex-native-writer-schema-certification"
        );
        assert_eq!(report.scoped_runtime_row_count(), 5);
        assert_eq!(report.provider_candidate_row_count(), 2);
        assert_eq!(report.blocked_row_count(), 1);
        assert!(report.local_runtime_claim_allowed);
        assert!(!report.performance_claim_allowed);
        assert!(!report.broad_schema_encoding_certification_complete);
        assert!(!report.metadata_statistics_broadly_certified);
        assert!(report.no_external_fallback());
        assert!(
            report
                .scoped_runtime_row_ids()
                .contains(&"flat_scalar_rows_nullable_primitives")
        );
        assert!(
            report
                .scoped_runtime_row_ids()
                .contains(&"typed_complex_scalar_rows_arrow_provider")
        );
        assert!(
            report
                .scoped_runtime_row_ids()
                .contains(&"flat_columnar_source_state_arrow_provider")
        );
        assert!(
            report
                .scoped_runtime_row_ids()
                .contains(&"nullable_columnar_validity_provider_gate")
        );
        assert!(
            report
                .scoped_runtime_row_ids()
                .contains(&"dictionary_encoded_utf8_binary_provider_gate")
        );
        assert!(
            report
                .provider_candidate_row_ids()
                .contains(&"dictionary_encoded_primitives_provider_gate")
        );
        assert!(
            report
                .provider_candidate_row_ids()
                .contains(&"extension_dtype_json_wkb_provider_gate")
        );
        assert!(
            report
                .blocked_row_ids()
                .contains(&"generalized_schema_encoding_writer")
        );
        let generalized = report
            .rows
            .iter()
            .find(|row| row.row_id == "generalized_schema_encoding_writer")
            .expect("generalized writer row is present");
        assert_eq!(
            generalized.status,
            VortexNativeWriterCertificationStatus::BlockedPendingEvidence
        );
        assert_eq!(
            generalized.unsupported_diagnostic_code,
            "SL_UNSUPPORTED_GENERALIZED_VORTEX_PAYLOAD_WRITE"
        );
        assert!(!generalized.local_write_runtime);
        assert!(!generalized.fallback_attempted);
        assert!(!generalized.external_engine_invoked);
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn object_store_io_gate_blocks_provider_credentials_and_writes() {
        let report = VortexObjectStoreIoGateReport::current();

        assert_eq!(report.gar_id, "GAR-0005-B");
        assert_eq!(
            report.schema_version,
            "shardloom.vortex_object_store_io_gate.v1"
        );
        assert_eq!(report.report_id, "gar0005b.vortex_object_store_io.gate");
        assert_eq!(report.gate_status, "report_only");
        assert_eq!(report.support_status, "unsupported");
        assert_eq!(report.rows.len(), 8);
        assert_eq!(report.unsupported_surface_count(), 7);
        assert_eq!(report.report_only_surface_count(), 1);
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert!(report.side_effect_free());
        assert!(report.claim_blocked());
        assert!(!report.has_errors());
        assert!(!report.object_store_read_execution_allowed);
        assert!(!report.object_store_write_execution_allowed);
        assert!(!report.upstream_vortex_read_allowed);
        assert!(!report.upstream_vortex_write_allowed);
        assert!(!report.credential_resolution_allowed);
        assert!(!report.provider_probe);
        assert!(!report.network_probe);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
    }

    #[test]
    fn object_store_io_gate_lists_stable_rows_and_diagnostics() {
        let report = VortexObjectStoreIoGateReport::current();

        assert_eq!(
            report.row_order(),
            vec![
                "object_store_vortex_read_provider",
                "object_store_vortex_write_provider",
                "credential_policy",
                "range_request_budget",
                "write_idempotency",
                "upstream_sink_api",
                "native_io_certificate",
                "unsupported_diagnostic",
            ]
        );
        assert_eq!(
            report.unsupported_diagnostic_count(),
            report.unsupported_surface_count()
        );
        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_OBJECT_STORE_UNSUPPORTED",
                "SL_OBJECT_STORE_UNSUPPORTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
                "SL_RESOURCE_BUDGET_EXCEEDED",
                "SL_COMMIT_NOT_ATOMIC",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
            ]
        );
        assert!(
            report
                .required_policy_refs
                .contains("idempotency_key_contract")
        );
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn vortex075_heavy_operator_disposition_maps_provider_candidates() {
        let report = Vortex075HeavyOperatorProviderDispositionReport::current();

        assert_eq!(
            report.schema_version,
            "shardloom.vortex075_heavy_operator_provider_disposition.v1"
        );
        assert_eq!(report.phase_id, "PERF-RUNTIME-7B");
        assert_eq!(report.provider_candidate_count(), 5);
        assert_eq!(report.wrapped_shardloom_kernel_count(), 1);
        assert_eq!(report.blocked_external_integration_count(), 1);
        assert_eq!(
            report.row_order(),
            vec![
                "grouped_sum_count_aggregate",
                "validity_mask_no_null",
                "branchless_zip",
                "dictionary_fsst_reuse",
                "layout_child_cache",
                "byte_length_expression",
                "datafusion_54_integration",
            ]
        );
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.runtime_execution);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn vortex075_heavy_operator_disposition_blocks_datafusion_runtime() {
        let report = Vortex075HeavyOperatorProviderDispositionReport::current();
        let datafusion = report
            .rows
            .iter()
            .find(|row| row.surface == Vortex075HeavyOperatorSurface::DataFusion54Integration)
            .expect("datafusion row");

        assert_eq!(
            datafusion.status,
            Vortex075HeavyOperatorDispositionStatus::BlockedExternalIntegration
        );
        assert_eq!(
            datafusion.shardloom_disposition,
            "baseline_or_oracle_only_not_shardloom_runtime_provider"
        );
        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(
            report.diagnostics[0].code,
            DiagnosticCode::NoFallbackExecution
        );
        assert!(!report.diagnostics[0].fallback.attempted);
        assert!(!report.diagnostics[0].fallback.allowed);
    }

    #[test]
    fn vortex075_local_io_disposition_maps_v1_provider_candidates() {
        let report = Vortex075LocalIoProviderDispositionReport::current();

        assert_eq!(
            report.schema_version,
            "shardloom.vortex075_local_io_provider_disposition.v1"
        );
        assert_eq!(report.phase_id, "PROD-READY-1A");
        assert_eq!(report.provider_candidate_count(), 7);
        assert_eq!(report.blocked_future_device_count(), 1);
        assert_eq!(report.deterministic_blocker_required_count(), 8);
        assert_eq!(
            report.row_order(),
            vec![
                "layout_reader_context_cache",
                "json_extension_arrow_interop",
                "wkb_geospatial_extension",
                "interleave_encoding",
                "binary_zstd_compression",
                "row_byte_encoder",
                "validity_mask_semantics",
                "arrow_device_gpu_path",
            ]
        );
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.runtime_execution);
        assert!(!report.data_read);
        assert!(!report.data_written);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn vortex075_local_io_disposition_blocks_device_paths_from_v1_runtime() {
        let report = Vortex075LocalIoProviderDispositionReport::current();
        let device = report
            .rows
            .iter()
            .find(|row| row.surface == Vortex075LocalIoSurface::ArrowDeviceGpuPath)
            .expect("device row");

        assert_eq!(
            device.status,
            Vortex075LocalIoDispositionStatus::BlockedFutureDeviceTrack
        );
        assert_eq!(
            device.shardloom_disposition,
            "blocked_future_device_track_not_local_cpu_v1"
        );
        assert!(!device.runtime_execution_allowed);
        assert!(!device.external_engine_invoked);
        assert!(!device.fallback_attempted);
        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::NotImplemented);
        assert_eq!(report.diagnostics[0].category, DiagnosticCategory::VortexIo);
        assert!(!report.diagnostics[0].fallback.attempted);
        assert!(!report.diagnostics[0].fallback.allowed);
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
