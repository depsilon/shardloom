//! Machine-readable CLI output envelope.
//!
//! Text rendering is for humans, JSON rendering is for agents/automation.
//! This module only renders output metadata and diagnostics; it does not execute work.

use crate::{Diagnostic, DiagnosticSeverity, FallbackStatus, Result, ShardLoomError};
use std::fmt::Write as _;

/// Current machine-readable CLI output schema.
pub const OUTPUT_ENVELOPE_SCHEMA_VERSION: &str = "shardloom.output.v2";

/// Output rendering format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }

    /// Parses CLI output format.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] for unsupported values.
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported output format: {value}"
            ))),
        }
    }
}

/// Top-level command status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    Success,
    Warning,
    Error,
    Unsupported,
}

impl CommandStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error | Self::Unsupported)
    }
}

/// User-visible `ShardLoom` execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomExecutionMode {
    Auto,
    CompatibilityImportCertified,
    PreparedVortex,
    DirectCompatibilityTransient,
    NativeVortex,
}

impl ShardLoomExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::CompatibilityImportCertified => "compatibility_import_certified",
            Self::PreparedVortex => "prepared_vortex",
            Self::DirectCompatibilityTransient => "direct_compatibility_transient",
            Self::NativeVortex => "native_vortex",
        }
    }

    #[must_use]
    pub const fn family(self) -> ShardLoomExecutionModeFamily {
        match self {
            Self::Auto => ShardLoomExecutionModeFamily::AutoSelection,
            Self::CompatibilityImportCertified | Self::DirectCompatibilityTransient => {
                ShardLoomExecutionModeFamily::Compatibility
            }
            Self::PreparedVortex | Self::NativeVortex => ShardLoomExecutionModeFamily::NativeVortex,
        }
    }

    /// Parses a user-visible execution mode.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] for unsupported values.
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "compatibility_import_certified" | "compatibility-import-certified" => {
                Ok(Self::CompatibilityImportCertified)
            }
            "prepared_vortex" | "prepared-vortex" => Ok(Self::PreparedVortex),
            "direct_compatibility_transient" | "direct-compatibility-transient" => {
                Ok(Self::DirectCompatibilityTransient)
            }
            "native_vortex" | "native-vortex" => Ok(Self::NativeVortex),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported ShardLoom execution mode: {value}; fallback execution was not attempted"
            ))),
        }
    }
}

/// Coarse execution-mode family used by benchmark and protocol surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomExecutionModeFamily {
    AutoSelection,
    Compatibility,
    NativeVortex,
}

impl ShardLoomExecutionModeFamily {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AutoSelection => "auto_selection",
            Self::Compatibility => "compatibility",
            Self::NativeVortex => "native_vortex",
        }
    }
}

pub const EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION: &str =
    "shardloom.execution_mode_selection_report.v1";

/// Provider-neutral request used to select and explain a `ShardLoom` execution mode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomExecutionModeSelectionRequest {
    pub requested_execution_mode: ShardLoomExecutionMode,
    pub source_format: String,
    pub workload_constitution_id: String,
    pub compatibility_input: bool,
    pub source_already_vortex: bool,
    pub certification_requested: bool,
    pub result_sink_requested: bool,
    pub prepared_artifact_available: bool,
    pub prepared_artifact_reuse_requested: bool,
    pub native_vortex_provider_available: bool,
    pub direct_transient_supported: bool,
}

impl ShardLoomExecutionModeSelectionRequest {
    #[must_use]
    pub fn new(requested_execution_mode: ShardLoomExecutionMode) -> Self {
        Self {
            requested_execution_mode,
            source_format: "unknown".to_string(),
            workload_constitution_id: "unknown".to_string(),
            compatibility_input: false,
            source_already_vortex: false,
            certification_requested: false,
            result_sink_requested: false,
            prepared_artifact_available: false,
            prepared_artifact_reuse_requested: false,
            native_vortex_provider_available: false,
            direct_transient_supported: false,
        }
    }

    #[must_use]
    pub fn with_source_format(mut self, value: impl Into<String>) -> Self {
        self.source_format = value.into();
        self
    }

    #[must_use]
    pub fn with_workload_constitution(mut self, value: impl Into<String>) -> Self {
        self.workload_constitution_id = value.into();
        self
    }

    #[must_use]
    pub const fn with_compatibility_input(mut self, value: bool) -> Self {
        self.compatibility_input = value;
        self
    }

    #[must_use]
    pub const fn with_source_already_vortex(mut self, value: bool) -> Self {
        self.source_already_vortex = value;
        self
    }

    #[must_use]
    pub const fn with_certification_requested(mut self, value: bool) -> Self {
        self.certification_requested = value;
        self
    }

    #[must_use]
    pub const fn with_result_sink_requested(mut self, value: bool) -> Self {
        self.result_sink_requested = value;
        self
    }

    #[must_use]
    pub const fn with_prepared_artifact_available(mut self, value: bool) -> Self {
        self.prepared_artifact_available = value;
        self
    }

    #[must_use]
    pub const fn with_prepared_artifact_reuse_requested(mut self, value: bool) -> Self {
        self.prepared_artifact_reuse_requested = value;
        self
    }

    #[must_use]
    pub const fn with_native_vortex_provider_available(mut self, value: bool) -> Self {
        self.native_vortex_provider_available = value;
        self
    }

    #[must_use]
    pub const fn with_direct_transient_supported(mut self, value: bool) -> Self {
        self.direct_transient_supported = value;
        self
    }
}

/// Deterministic execution-mode selection report shared by CLI, Python, benchmarks,
/// and future REST surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomExecutionModeSelectionReport {
    pub schema_version: &'static str,
    pub requested_execution_mode: ShardLoomExecutionMode,
    pub selected_execution_mode: ShardLoomExecutionMode,
    pub mode_selection_reason: String,
    pub execution_mode_family: ShardLoomExecutionModeFamily,
    pub source_format: String,
    pub workload_constitution_id: String,
    pub compatibility_import_included: bool,
    pub vortex_prepare_included: bool,
    pub vortex_write_reopen_included: bool,
    pub direct_transient_execution: bool,
    pub vortex_native_claim_allowed: bool,
    pub certification_requested: bool,
    pub result_sink_requested: bool,
    pub prepared_artifact_available: bool,
    pub native_vortex_provider_available: bool,
    pub mode_supported: bool,
    pub support_status: String,
    pub unsupported_diagnostic_code: String,
    pub blocker_id: String,
    pub required_future_evidence: String,
    pub claim_gate_status: String,
    pub claim_gate_reason: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct SupportedExecutionModeFacts<'a> {
    compatibility_import_included: bool,
    vortex_prepare_included: bool,
    vortex_write_reopen_included: bool,
    direct_transient_execution: bool,
    vortex_native_claim_allowed: bool,
    claim_gate_status: &'a str,
    claim_gate_reason: &'a str,
}

impl ShardLoomExecutionModeSelectionReport {
    #[must_use]
    pub fn from_request(request: ShardLoomExecutionModeSelectionRequest) -> Self {
        match request.requested_execution_mode {
            ShardLoomExecutionMode::Auto => Self::select_auto(request),
            ShardLoomExecutionMode::CompatibilityImportCertified => {
                Self::compatibility_import_certified(
                    request,
                    "compatibility_import_certified_requested",
                )
            }
            ShardLoomExecutionMode::PreparedVortex => {
                if request.prepared_artifact_available || request.source_already_vortex {
                    Self::prepared_vortex(
                        request,
                        "prepared_vortex_artifacts_available_before_scenario_timing",
                    )
                } else {
                    Self::unsupported(
                        request,
                        ShardLoomExecutionMode::PreparedVortex,
                        "prepared_vortex_artifact_missing",
                        "P7.5.3",
                        "prepared Vortex artifact lifecycle evidence",
                    )
                }
            }
            ShardLoomExecutionMode::DirectCompatibilityTransient => {
                if request.direct_transient_supported {
                    Self::direct_compatibility_transient(
                        request,
                        "direct_transient_requested_and_supported",
                    )
                } else {
                    Self::unsupported(
                        request,
                        ShardLoomExecutionMode::DirectCompatibilityTransient,
                        "direct_compatibility_transient_not_implemented",
                        "P7.5.4",
                        "ShardLoom-native direct transient executor and direct-mode evidence",
                    )
                }
            }
            ShardLoomExecutionMode::NativeVortex => {
                if request.source_already_vortex || request.native_vortex_provider_available {
                    Self::native_vortex(request, "input_already_vortex")
                } else {
                    Self::unsupported(
                        request,
                        ShardLoomExecutionMode::NativeVortex,
                        "native_vortex_input_missing",
                        "P7.5.5",
                        "native Vortex input or admitted native provider evidence",
                    )
                }
            }
        }
    }

    fn select_auto(request: ShardLoomExecutionModeSelectionRequest) -> Self {
        if request.source_already_vortex && request.native_vortex_provider_available {
            return Self::native_vortex(request, "auto_selected_input_already_vortex");
        }
        if request.prepared_artifact_available || request.prepared_artifact_reuse_requested {
            return Self::prepared_vortex(request, "auto_selected_prepared_vortex_artifact_reuse");
        }
        if request.certification_requested || request.result_sink_requested {
            return Self::compatibility_import_certified(
                request,
                "auto_selected_certified_ingest_stage_requested",
            );
        }
        if request.compatibility_input && request.direct_transient_supported {
            return Self::direct_compatibility_transient(
                request,
                "auto_selected_direct_transient_small_compatibility_input",
            );
        }
        Self::compatibility_import_certified(
            request,
            "auto_selected_compatibility_import_certified_until_direct_transient_is_admitted",
        )
    }

    fn compatibility_import_certified(
        request: ShardLoomExecutionModeSelectionRequest,
        reason: &str,
    ) -> Self {
        Self::supported(
            request,
            ShardLoomExecutionMode::CompatibilityImportCertified,
            reason,
            SupportedExecutionModeFacts {
                compatibility_import_included: true,
                vortex_prepare_included: true,
                vortex_write_reopen_included: true,
                direct_transient_execution: false,
                vortex_native_claim_allowed: false,
                claim_gate_status: "not_claim_grade",
                claim_gate_reason: "compatibility_import_certified_ingest_stage_not_pure_compute",
            },
        )
    }

    fn prepared_vortex(request: ShardLoomExecutionModeSelectionRequest, reason: &str) -> Self {
        Self::supported(
            request,
            ShardLoomExecutionMode::PreparedVortex,
            reason,
            SupportedExecutionModeFacts {
                compatibility_import_included: false,
                vortex_prepare_included: false,
                vortex_write_reopen_included: false,
                direct_transient_execution: false,
                vortex_native_claim_allowed: true,
                claim_gate_status: "fixture_smoke_only",
                claim_gate_reason: "prepared_vortex_requires_operator_and_certificate_evidence_for_claim_grade",
            },
        )
    }

    fn direct_compatibility_transient(
        request: ShardLoomExecutionModeSelectionRequest,
        reason: &str,
    ) -> Self {
        Self::supported(
            request,
            ShardLoomExecutionMode::DirectCompatibilityTransient,
            reason,
            SupportedExecutionModeFacts {
                compatibility_import_included: false,
                vortex_prepare_included: false,
                vortex_write_reopen_included: false,
                direct_transient_execution: true,
                vortex_native_claim_allowed: false,
                claim_gate_status: "not_claim_grade",
                claim_gate_reason: "not_vortex_native",
            },
        )
    }

    fn native_vortex(request: ShardLoomExecutionModeSelectionRequest, reason: &str) -> Self {
        Self::supported(
            request,
            ShardLoomExecutionMode::NativeVortex,
            reason,
            SupportedExecutionModeFacts {
                compatibility_import_included: false,
                vortex_prepare_included: false,
                vortex_write_reopen_included: false,
                direct_transient_execution: false,
                vortex_native_claim_allowed: true,
                claim_gate_status: "fixture_smoke_only",
                claim_gate_reason: "native_vortex_operator_evidence_required_for_claim_grade",
            },
        )
    }

    fn supported(
        request: ShardLoomExecutionModeSelectionRequest,
        selected_execution_mode: ShardLoomExecutionMode,
        reason: &str,
        facts: SupportedExecutionModeFacts<'_>,
    ) -> Self {
        Self {
            schema_version: EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION,
            requested_execution_mode: request.requested_execution_mode,
            selected_execution_mode,
            mode_selection_reason: reason.to_string(),
            execution_mode_family: selected_execution_mode.family(),
            source_format: request.source_format,
            workload_constitution_id: request.workload_constitution_id,
            compatibility_import_included: facts.compatibility_import_included,
            vortex_prepare_included: facts.vortex_prepare_included,
            vortex_write_reopen_included: facts.vortex_write_reopen_included,
            direct_transient_execution: facts.direct_transient_execution,
            vortex_native_claim_allowed: facts.vortex_native_claim_allowed,
            certification_requested: request.certification_requested,
            result_sink_requested: request.result_sink_requested,
            prepared_artifact_available: request.prepared_artifact_available,
            native_vortex_provider_available: request.native_vortex_provider_available,
            mode_supported: true,
            support_status: "supported".to_string(),
            unsupported_diagnostic_code: "none".to_string(),
            blocker_id: "none".to_string(),
            required_future_evidence: "none".to_string(),
            claim_gate_status: facts.claim_gate_status.to_string(),
            claim_gate_reason: facts.claim_gate_reason.to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn unsupported(
        request: ShardLoomExecutionModeSelectionRequest,
        selected_execution_mode: ShardLoomExecutionMode,
        diagnostic_code: &str,
        blocker_id: &str,
        required_future_evidence: &str,
    ) -> Self {
        Self {
            schema_version: EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION,
            requested_execution_mode: request.requested_execution_mode,
            selected_execution_mode,
            mode_selection_reason: diagnostic_code.to_string(),
            execution_mode_family: selected_execution_mode.family(),
            source_format: request.source_format,
            workload_constitution_id: request.workload_constitution_id,
            compatibility_import_included: false,
            vortex_prepare_included: false,
            vortex_write_reopen_included: false,
            direct_transient_execution: false,
            vortex_native_claim_allowed: false,
            certification_requested: request.certification_requested,
            result_sink_requested: request.result_sink_requested,
            prepared_artifact_available: request.prepared_artifact_available,
            native_vortex_provider_available: request.native_vortex_provider_available,
            mode_supported: false,
            support_status: "unsupported".to_string(),
            unsupported_diagnostic_code: diagnostic_code.to_string(),
            blocker_id: blocker_id.to_string(),
            required_future_evidence: required_future_evidence.to_string(),
            claim_gate_status: "not_claim_grade".to_string(),
            claim_gate_reason: diagnostic_code.to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "execution_mode_selection_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "requested_execution_mode".to_string(),
                self.requested_execution_mode.as_str().to_string(),
            ),
            (
                "selected_execution_mode".to_string(),
                self.selected_execution_mode.as_str().to_string(),
            ),
            (
                "execution_mode".to_string(),
                self.selected_execution_mode.as_str().to_string(),
            ),
            (
                "mode_selection_reason".to_string(),
                self.mode_selection_reason.clone(),
            ),
            (
                "execution_mode_family".to_string(),
                self.execution_mode_family.as_str().to_string(),
            ),
            ("source_format".to_string(), self.source_format.clone()),
            (
                "workload_constitution_id".to_string(),
                self.workload_constitution_id.clone(),
            ),
            (
                "compatibility_import_included".to_string(),
                self.compatibility_import_included.to_string(),
            ),
            (
                "vortex_prepare_included".to_string(),
                self.vortex_prepare_included.to_string(),
            ),
            (
                "vortex_write_reopen_included".to_string(),
                self.vortex_write_reopen_included.to_string(),
            ),
            (
                "direct_transient_execution".to_string(),
                self.direct_transient_execution.to_string(),
            ),
            (
                "vortex_native_claim_allowed".to_string(),
                self.vortex_native_claim_allowed.to_string(),
            ),
            (
                "certification_requested".to_string(),
                self.certification_requested.to_string(),
            ),
            (
                "result_sink_requested".to_string(),
                self.result_sink_requested.to_string(),
            ),
            (
                "prepared_artifact_available".to_string(),
                self.prepared_artifact_available.to_string(),
            ),
            (
                "native_vortex_provider_available".to_string(),
                self.native_vortex_provider_available.to_string(),
            ),
            (
                "mode_supported".to_string(),
                self.mode_supported.to_string(),
            ),
            ("support_status".to_string(), self.support_status.clone()),
            (
                "unsupported_diagnostic_code".to_string(),
                self.unsupported_diagnostic_code.clone(),
            ),
            ("blocker_id".to_string(), self.blocker_id.clone()),
            (
                "required_future_evidence".to_string(),
                self.required_future_evidence.clone(),
            ),
            (
                "claim_gate_status".to_string(),
                self.claim_gate_status.clone(),
            ),
            (
                "claim_gate_reason".to_string(),
                self.claim_gate_reason.clone(),
            ),
            (
                "fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
        ]
    }
}

/// Typed key/value payload used inside explicit envelope slots.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OutputTypedPayload {
    pub fields: Vec<(String, String)>,
}

impl OutputTypedPayload {
    /// Creates an empty typed payload.
    #[must_use]
    pub const fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    /// Adds a stable machine-readable field to this payload.
    pub fn add_field(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.fields.push((key.into(), value.into()));
    }

    #[must_use]
    fn to_json(&self) -> String {
        format!("{{\"fields\":[{}]}}", key_value_fields_json(&self.fields))
    }
}

/// Typed reference to a result, artifact, or certificate carried by the envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputTypedRef {
    pub id: String,
    pub kind: String,
    pub status: String,
    pub uri: Option<String>,
}

impl OutputTypedRef {
    /// Creates a typed envelope reference.
    #[must_use]
    pub fn new(id: impl Into<String>, kind: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: kind.into(),
            status: status.into(),
            uri: None,
        }
    }

    /// Adds an optional URI/path/reference target.
    #[must_use]
    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    #[must_use]
    fn to_json(&self) -> String {
        format!(
            "{{\"id\":{},\"kind\":{},\"status\":{},\"uri\":{}}}",
            json_string(&self.id),
            json_string(&self.kind),
            json_string(&self.status),
            json_optional_string(self.uri.as_deref()),
        )
    }
}

/// Inline typed artifact payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputTypedArtifact {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub status: String,
    pub payload: OutputTypedPayload,
}

impl OutputTypedArtifact {
    /// Creates an inline artifact with an empty payload.
    #[must_use]
    pub fn new(
        artifact_id: impl Into<String>,
        artifact_kind: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            artifact_kind: artifact_kind.into(),
            status: status.into(),
            payload: OutputTypedPayload::empty(),
        }
    }

    /// Adds a stable machine-readable field to the artifact payload.
    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.payload.add_field(key, value);
        self
    }

    #[must_use]
    fn to_json(&self) -> String {
        format!(
            "{{\"artifact_id\":{},\"artifact_kind\":{},\"status\":{},\"payload\":{}}}",
            json_string(&self.artifact_id),
            json_string(&self.artifact_kind),
            json_string(&self.status),
            self.payload.to_json(),
        )
    }
}

/// Stable command envelope for machine-readable CLI output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEnvelope {
    pub schema_version: &'static str,
    pub command: String,
    pub status: CommandStatus,
    pub summary: String,
    pub human_text: String,
    pub fallback: FallbackStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub result: OutputTypedPayload,
    pub result_refs: Vec<OutputTypedRef>,
    pub artifacts: Vec<OutputTypedArtifact>,
    pub artifact_refs: Vec<OutputTypedRef>,
    pub certificates: Vec<OutputTypedRef>,
    pub policy: OutputTypedPayload,
    pub lifecycle: OutputTypedPayload,
    pub capability_snapshot: OutputTypedPayload,
    /// Deprecated key/value mirror retained only while existing command families
    /// are migrated to typed handlers. The primary machine-readable payload is
    /// `result` plus typed refs/artifact/certificate/policy/lifecycle slots.
    pub fields: Vec<(String, String)>,
}

impl OutputEnvelope {
    #[must_use]
    pub fn new(
        command: impl Into<String>,
        status: CommandStatus,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: OUTPUT_ENVELOPE_SCHEMA_VERSION,
            command: command.into(),
            status,
            summary: summary.into(),
            human_text: human_text.into(),
            fallback: FallbackStatus::disabled_by_policy(),
            diagnostics: Vec::new(),
            result: OutputTypedPayload::empty(),
            result_refs: Vec::new(),
            artifacts: Vec::new(),
            artifact_refs: Vec::new(),
            certificates: Vec::new(),
            policy: OutputTypedPayload::empty(),
            lifecycle: OutputTypedPayload::empty(),
            capability_snapshot: OutputTypedPayload::empty(),
            fields: Vec::new(),
        }
    }

    #[must_use]
    pub fn success(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self::new(command, CommandStatus::Success, summary, human_text)
    }

    #[must_use]
    pub fn unsupported(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self::new(command, CommandStatus::Unsupported, summary, human_text)
    }

    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.result.add_field(key.clone(), value.clone());
        self.fields.push((key, value));
        self
    }

    #[must_use]
    pub fn with_result_field(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_field(key, value)
    }

    #[must_use]
    pub fn with_legacy_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    #[must_use]
    pub fn with_result_ref(mut self, reference: OutputTypedRef) -> Self {
        self.result_refs.push(reference);
        self
    }

    #[must_use]
    pub fn with_artifact(mut self, artifact: OutputTypedArtifact) -> Self {
        self.artifacts.push(artifact);
        self
    }

    #[must_use]
    pub fn with_artifact_ref(mut self, reference: OutputTypedRef) -> Self {
        self.artifact_refs.push(reference);
        self
    }

    #[must_use]
    pub fn with_certificate(mut self, reference: OutputTypedRef) -> Self {
        self.certificates.push(reference);
        self
    }

    #[must_use]
    pub fn with_policy_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.policy.add_field(key, value);
        self
    }

    #[must_use]
    pub fn with_lifecycle_field(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.lifecycle.add_field(key, value);
        self
    }

    #[must_use]
    pub fn with_capability_snapshot_field(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.capability_snapshot.add_field(key, value);
        self
    }

    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Builds an envelope from a diagnostic while preserving structured fallback status.
    ///
    /// This is intended for deterministic agent-facing JSON and human-facing text output
    /// without adding serialization dependencies.
    #[must_use]
    pub fn from_diagnostic(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
        diagnostic: Diagnostic,
    ) -> Self {
        let status = if matches!(
            diagnostic.category,
            crate::DiagnosticCategory::UnsupportedFeature
                | crate::DiagnosticCategory::NoFallbackPolicy
        ) {
            CommandStatus::Unsupported
        } else {
            match diagnostic.severity {
                DiagnosticSeverity::Fatal | DiagnosticSeverity::Error => CommandStatus::Error,
                DiagnosticSeverity::Warning => CommandStatus::Warning,
                DiagnosticSeverity::Info => CommandStatus::Success,
            }
        };
        let fallback = diagnostic.fallback.clone();
        let mut envelope =
            Self::new(command, status, summary, human_text).with_diagnostic(diagnostic);
        envelope.fallback = fallback;
        envelope
    }

    /// Builds an envelope from a plain error by converting it to a structured diagnostic.
    ///
    /// Plain errors should be normalized before user-facing rendering so text/json output
    /// remains stable for both humans and agents.
    #[must_use]
    pub fn from_error(
        command: impl Into<String>,
        summary: impl Into<String>,
        error: &ShardLoomError,
    ) -> Self {
        let diagnostic = error.to_diagnostic();
        Self::from_diagnostic(command, summary, error.to_string(), diagnostic)
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
    pub fn to_text(&self) -> String {
        self.human_text.clone()
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let diagnostics = self
            .diagnostics
            .iter()
            .map(Diagnostic::to_json)
            .collect::<Vec<_>>()
            .join(",");
        let result_refs = typed_ref_array_json(&self.result_refs);
        let artifacts = self
            .artifacts
            .iter()
            .map(OutputTypedArtifact::to_json)
            .collect::<Vec<_>>()
            .join(",");
        let artifact_refs = typed_ref_array_json(&self.artifact_refs);
        let certificates = typed_ref_array_json(&self.certificates);
        let fields = key_value_fields_json(&self.fields);
        format!(
            "{{\"schema_version\":{},\"command\":{},\"status\":{},\"summary\":{},\"human_text\":{},\"fallback\":{},\"diagnostics\":[{}],\"result\":{},\"result_refs\":[{}],\"artifacts\":[{}],\"artifact_refs\":[{}],\"certificates\":[{}],\"policy\":{},\"lifecycle\":{},\"capability_snapshot\":{},\"fields\":[{}]}}",
            json_string(self.schema_version),
            json_string(&self.command),
            json_string(self.status.as_str()),
            json_string(&self.summary),
            json_string(&self.human_text),
            self.fallback.to_json(),
            diagnostics,
            self.result.to_json(),
            result_refs,
            artifacts,
            artifact_refs,
            certificates,
            self.policy.to_json(),
            self.lifecycle.to_json(),
            self.capability_snapshot.to_json(),
            fields,
        )
    }

    #[must_use]
    pub fn render(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Text => self.to_text(),
            OutputFormat::Json => self.to_json(),
        }
    }
}

/// Report-only CG-11 contract for the stable CLI/API JSON protocol foundation.
///
/// This is the protocol surface that a future thin Python wrapper or other
/// client can consume before native bindings exist. It describes the existing
/// [`OutputEnvelope`] JSON shape without executing commands, probing local
/// state, publishing packages, or enabling fallback execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliApiJsonProtocolReport {
    pub schema_version: &'static str,
    pub protocol_id: &'static str,
    pub protocol_stability: &'static str,
    pub output_envelope_schema_version: &'static str,
    pub required_envelope_fields: Vec<&'static str>,
    pub required_fallback_fields: Vec<&'static str>,
    pub required_diagnostic_fields: Vec<&'static str>,
    pub required_field_entry_fields: Vec<&'static str>,
    pub required_typed_payload_fields: Vec<&'static str>,
    pub legacy_fields_mirror_present: bool,
    pub flat_fields_primary_payload_allowed: bool,
    pub command_status_values: Vec<&'static str>,
    pub compatibility_lock_status: &'static str,
    pub compatibility_lock_fixture_statuses: Vec<&'static str>,
    pub json_error_paths_enveloped: bool,
    pub unknown_command_json_enveloped: bool,
    pub missing_binary_error_payload_shaped: bool,
    pub output_formats: Vec<&'static str>,
    pub thin_python_wrapper_boundary: &'static str,
    pub pyo3_maturin_allowed: bool,
    pub foundry_required: bool,
    pub dataframe_api_implemented: bool,
    pub side_effect_free: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub parser_executed: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_publish: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// Report-only CG-11 contract for the first thin Python wrapper boundary.
///
/// The foundation wrapper is a source-tree client over the CLI JSON protocol,
/// not a native Python binding, package publication, `DataFrame`
/// implementation, UDF runtime, or hidden fallback execution path.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonWrapperFoundationReport {
    pub schema_version: &'static str,
    pub wrapper_id: &'static str,
    pub wrapper_status: &'static str,
    pub transport_protocol_id: &'static str,
    pub output_envelope_schema_version: &'static str,
    pub invocation_model: &'static str,
    pub initial_command_scope: Vec<&'static str>,
    pub required_client_behaviors: Vec<&'static str>,
    pub package_status: &'static str,
    pub native_binding_status: &'static str,
    pub wheel_sdist_build_ready: bool,
    pub fresh_environment_smoke_required: bool,
    pub missing_binary_diagnostic_ready: bool,
    pub conda_cli_package_required: bool,
    pub conda_python_package_planned: bool,
    pub conda_metapackage_planned: bool,
    pub conda_recipe_root: &'static str,
    pub conda_cli_recipe_created: bool,
    pub conda_python_recipe_created: bool,
    pub conda_metapackage_recipe_created: bool,
    pub benchmark_extras_optional: bool,
    pub pyo3_maturin_allowed: bool,
    pub python_package_created: bool,
    pub native_extension_required: bool,
    pub dataframe_api_implemented: bool,
    pub notebook_api_implemented: bool,
    pub python_udf_runtime_implemented: bool,
    pub materialization_boundary_reporting_required: bool,
    pub diagnostics_passthrough_required: bool,
    pub side_effect_free: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub parser_executed: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_publish: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl PythonWrapperFoundationReport {
    /// Builds the static Python wrapper foundation report without invoking
    /// Python, probing the host, publishing packages, or executing runtime work.
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.python_wrapper_foundation.v1",
            wrapper_id: "shardloom_python_cli_json_client",
            wrapper_status: "source_tree_foundation",
            transport_protocol_id: "shardloom.cli_json.v1",
            output_envelope_schema_version: OUTPUT_ENVELOPE_SCHEMA_VERSION,
            invocation_model: "subprocess_cli_json",
            initial_command_scope: vec![
                "status",
                "capabilities",
                "api-compat-plan",
                "python-wrapper-plan",
                "vortex-run",
                "traditional-analytics-run",
                "traditional-analytics-vortex-run",
                "traditional-analytics-vortex-batch-run",
                "traditional-analytics-prepare-batch-run",
                "dynamic-work-shaping-plan",
                "sizing-feedback-plan",
                "benchmark-plan",
                "benchmark-claim-evidence-plan",
                "benchmark-constitution",
            ],
            required_client_behaviors: vec![
                "invoke_shardloom_with_format_json",
                "parse_output_envelope",
                "preserve_diagnostics",
                "preserve_fallback_status",
                "surface_materialization_boundaries",
                "do_not_probe_on_import",
                "resolve_local_binary_only_on_explicit_client_use",
                "raise_deterministic_missing_binary_error",
                "treat_runtime_commands_as_explicit_user_invocations",
                "do_not_retry_as_fallback_engine",
            ],
            package_status: "source_tree_wheel_sdist_ready",
            native_binding_status: "not_created",
            wheel_sdist_build_ready: true,
            fresh_environment_smoke_required: true,
            missing_binary_diagnostic_ready: true,
            conda_cli_package_required: true,
            conda_python_package_planned: true,
            conda_metapackage_planned: true,
            conda_recipe_root: "packaging/conda",
            conda_cli_recipe_created: true,
            conda_python_recipe_created: true,
            conda_metapackage_recipe_created: true,
            benchmark_extras_optional: true,
            pyo3_maturin_allowed: false,
            python_package_created: true,
            native_extension_required: false,
            dataframe_api_implemented: false,
            notebook_api_implemented: false,
            python_udf_runtime_implemented: false,
            materialization_boundary_reporting_required: true,
            diagnostics_passthrough_required: true,
            side_effect_free: true,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            parser_executed: false,
            runtime_execution: false,
            write_io: false,
            external_publish: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.pyo3_maturin_allowed
            || self.native_extension_required
            || self.dataframe_api_implemented
            || self.notebook_api_implemented
            || self.python_udf_runtime_implemented
            || !self.side_effect_free
            || self.filesystem_probe
            || self.network_probe
            || self.catalog_probe
            || self.adapter_probe
            || self.parser_executed
            || self.runtime_execution
            || self.write_io
            || self.external_publish
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
            "python wrapper foundation\nschema_version: {}\nwrapper_id: {}\nwrapper_status: {}\ntransport_protocol_id: {}\noutput_envelope_schema_version: {}\ninvocation_model: {}\ninitial command scope: {}\nrequired client behaviors: {}\npackage_status: {}\nnative_binding_status: {}\nwheel/sdist build ready: {}\nmissing binary diagnostics: {}\npyo3/maturin allowed: {}\npython package created: {}\nruntime execution: disabled\nwrite io: disabled\nfallback execution: disabled",
            self.schema_version,
            self.wrapper_id,
            self.wrapper_status,
            self.transport_protocol_id,
            self.output_envelope_schema_version,
            self.invocation_model,
            self.initial_command_scope.join(", "),
            self.required_client_behaviors.join(", "),
            self.package_status,
            self.native_binding_status,
            self.wheel_sdist_build_ready,
            self.missing_binary_diagnostic_ready,
            self.pyo3_maturin_allowed,
            self.python_package_created,
        )
    }
}

impl CliApiJsonProtocolReport {
    /// Builds the static protocol-foundation report without any probing.
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.cli_api_json_protocol.v1",
            protocol_id: "shardloom.cli_json.v1",
            protocol_stability: "experimental",
            output_envelope_schema_version: OUTPUT_ENVELOPE_SCHEMA_VERSION,
            required_envelope_fields: vec![
                "schema_version",
                "command",
                "status",
                "summary",
                "human_text",
                "fallback",
                "diagnostics",
                "result",
                "result_refs",
                "artifacts",
                "artifact_refs",
                "certificates",
                "policy",
                "lifecycle",
                "capability_snapshot",
                "fields",
            ],
            required_fallback_fields: vec!["attempted", "allowed", "engine", "reason"],
            required_diagnostic_fields: vec![
                "code",
                "severity",
                "category",
                "message",
                "feature",
                "reason",
                "suggested_next_step",
                "fallback",
            ],
            required_field_entry_fields: vec!["key", "value"],
            required_typed_payload_fields: vec!["fields"],
            legacy_fields_mirror_present: true,
            flat_fields_primary_payload_allowed: false,
            command_status_values: vec!["success", "warning", "error", "unsupported"],
            compatibility_lock_status: "locked",
            compatibility_lock_fixture_statuses: vec![
                "success",
                "error",
                "unsupported",
                "blocked",
                "evidence_incomplete",
                "certified_local_execution",
                "missing_binary",
                "foundry_optional",
            ],
            json_error_paths_enveloped: true,
            unknown_command_json_enveloped: true,
            missing_binary_error_payload_shaped: true,
            output_formats: vec!["text", "json"],
            thin_python_wrapper_boundary: "cli_json_subprocess_first",
            pyo3_maturin_allowed: false,
            foundry_required: false,
            dataframe_api_implemented: false,
            side_effect_free: true,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            parser_executed: false,
            runtime_execution: false,
            write_io: false,
            external_publish: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.fallback_execution_allowed
            || self.fallback_attempted
            || self.flat_fields_primary_payload_allowed
            || self.compatibility_lock_status != "locked"
            || !self.json_error_paths_enveloped
            || !self.unknown_command_json_enveloped
            || !self.missing_binary_error_payload_shaped
            || self.runtime_execution
            || self.write_io
            || self.external_publish
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
            "cli/api json protocol\nschema_version: {}\nprotocol_id: {}\nprotocol_stability: {}\noutput_envelope_schema_version: {}\nrequired envelope fields: {}\ncommand statuses: {}\noutput formats: {}\npython wrapper boundary: {}\npyo3/maturin allowed: {}\nfoundry required: {}\nruntime execution: disabled\nwrite io: disabled\nfallback execution: disabled",
            self.schema_version,
            self.protocol_id,
            self.protocol_stability,
            self.output_envelope_schema_version,
            self.required_envelope_fields.join(", "),
            self.command_status_values.join(", "),
            self.output_formats.join(", "),
            self.thin_python_wrapper_boundary,
            self.pyo3_maturin_allowed,
            self.foundry_required,
        )
    }
}

pub(crate) fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            c if c.is_control() => {
                let _ = write!(&mut escaped, "\\u{:04X}", c as u32);
            }
            c => escaped.push(c),
        }
    }
    escaped
}

pub(crate) fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

pub(crate) fn json_optional_string(value: Option<&str>) -> String {
    value.map_or_else(|| "null".to_string(), json_string)
}

fn key_value_fields_json(fields: &[(String, String)]) -> String {
    fields
        .iter()
        .map(|(key, value)| {
            format!(
                "{{\"key\":{},\"value\":{}}}",
                json_string(key),
                json_string(value)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn typed_ref_array_json(refs: &[OutputTypedRef]) -> String {
    refs.iter()
        .map(OutputTypedRef::to_json)
        .collect::<Vec<_>>()
        .join(",")
}

#[must_use]
pub(crate) const fn json_bool(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

#[cfg(test)]
mod tests {
    use crate::{Diagnostic, DiagnosticCode};

    use super::*;

    #[test]
    fn format_parses_text() {
        assert_eq!(OutputFormat::parse("text").expect("ok"), OutputFormat::Text);
    }
    #[test]
    fn format_parses_json() {
        assert_eq!(OutputFormat::parse("json").expect("ok"), OutputFormat::Json);
    }
    #[test]
    fn format_parsing_case_insensitive() {
        assert_eq!(OutputFormat::parse("JsOn").expect("ok"), OutputFormat::Json);
    }
    #[test]
    fn format_rejects_unknown() {
        assert!(OutputFormat::parse("yaml").is_err());
    }
    #[test]
    fn execution_mode_parse_and_family_are_stable() {
        assert_eq!(
            ShardLoomExecutionMode::parse("compatibility_import_certified").expect("ok"),
            ShardLoomExecutionMode::CompatibilityImportCertified
        );
        assert_eq!(
            ShardLoomExecutionMode::parse("prepared-vortex").expect("ok"),
            ShardLoomExecutionMode::PreparedVortex
        );
        assert_eq!(
            ShardLoomExecutionMode::PreparedVortex.family().as_str(),
            "native_vortex"
        );
        assert_eq!(
            ShardLoomExecutionMode::DirectCompatibilityTransient
                .family()
                .as_str(),
            "compatibility"
        );
        assert!(ShardLoomExecutionMode::parse("spark_fallback").is_err());
    }
    #[test]
    fn execution_mode_selection_auto_is_transparent_for_certified_ingest() {
        let report = ShardLoomExecutionModeSelectionReport::from_request(
            ShardLoomExecutionModeSelectionRequest::new(ShardLoomExecutionMode::Auto)
                .with_source_format("csv")
                .with_workload_constitution("local_vortex_analytics_v1")
                .with_compatibility_input(true)
                .with_certification_requested(true)
                .with_result_sink_requested(true),
        );

        assert_eq!(
            report.requested_execution_mode,
            ShardLoomExecutionMode::Auto
        );
        assert_eq!(
            report.selected_execution_mode,
            ShardLoomExecutionMode::CompatibilityImportCertified
        );
        assert_eq!(
            report.mode_selection_reason,
            "auto_selected_certified_ingest_stage_requested"
        );
        assert!(report.compatibility_import_included);
        assert!(report.vortex_prepare_included);
        assert!(!report.vortex_native_claim_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }
    #[test]
    fn execution_mode_selection_prepared_vortex_reports_query_timing_scope() {
        let report = ShardLoomExecutionModeSelectionReport::from_request(
            ShardLoomExecutionModeSelectionRequest::new(ShardLoomExecutionMode::PreparedVortex)
                .with_source_format("vortex")
                .with_workload_constitution("local_vortex_analytics_v1")
                .with_source_already_vortex(true)
                .with_prepared_artifact_available(true)
                .with_prepared_artifact_reuse_requested(true)
                .with_native_vortex_provider_available(true),
        );

        assert_eq!(
            report.selected_execution_mode,
            ShardLoomExecutionMode::PreparedVortex
        );
        assert_eq!(report.execution_mode_family.as_str(), "native_vortex");
        assert!(!report.compatibility_import_included);
        assert!(!report.vortex_prepare_included);
        assert!(report.vortex_native_claim_allowed);
        assert_eq!(report.support_status, "supported");
        assert_eq!(report.claim_gate_status, "fixture_smoke_only");
    }
    #[test]
    fn execution_mode_selection_blocks_direct_transient_until_implemented() {
        let report = ShardLoomExecutionModeSelectionReport::from_request(
            ShardLoomExecutionModeSelectionRequest::new(
                ShardLoomExecutionMode::DirectCompatibilityTransient,
            )
            .with_source_format("csv")
            .with_workload_constitution("local_vortex_analytics_v1")
            .with_compatibility_input(true),
        );

        assert!(!report.mode_supported);
        assert_eq!(report.support_status, "unsupported");
        assert_eq!(
            report.selected_execution_mode,
            ShardLoomExecutionMode::DirectCompatibilityTransient
        );
        assert_eq!(
            report.unsupported_diagnostic_code,
            "direct_compatibility_transient_not_implemented"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.vortex_native_claim_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }
    #[test]
    fn command_status_error_is_error() {
        assert!(CommandStatus::Error.is_error());
    }
    #[test]
    fn command_status_unsupported_is_error() {
        assert!(CommandStatus::Unsupported.is_error());
    }
    #[test]
    fn json_escape_escapes_quotes() {
        assert_eq!(json_escape("\""), "\\\"");
    }
    #[test]
    fn json_escape_escapes_backslashes() {
        assert_eq!(json_escape("\\"), "\\\\");
    }
    #[test]
    fn json_escape_escapes_newline() {
        assert_eq!(json_escape("\n"), "\\n");
    }
    #[test]
    fn json_escape_escapes_other_control_chars() {
        assert_eq!(json_escape("\u{0000}"), "\\u0000");
        assert_eq!(json_escape("\u{0008}"), "\\b");
        assert_eq!(json_escape("\u{000C}"), "\\f");
    }
    #[test]
    fn success_defaults_fallback_false() {
        assert!(
            !OutputEnvelope::success("status", "ok", "ok")
                .fallback
                .allowed
        );
    }
    #[test]
    fn has_errors_false_for_success_without_diagnostics() {
        assert!(!OutputEnvelope::success("status", "ok", "ok").has_errors());
    }
    #[test]
    fn unsupported_has_errors() {
        assert!(OutputEnvelope::unsupported("explain", "unsupported", "unsupported").has_errors());
    }
    #[test]
    fn to_json_includes_schema_version() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"schema_version\"")
        );
    }
    #[test]
    fn to_json_includes_fallback() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"fallback\"")
        );
    }
    #[test]
    fn to_json_includes_diagnostics_array() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"diagnostics\":[")
        );
    }
    #[test]
    fn to_json_includes_typed_payload_slots() {
        let json = OutputEnvelope::success("status", "ok", "ok")
            .with_field("engine", "shardloom")
            .with_result_ref(OutputTypedRef::new("result.local", "json", "available"))
            .with_artifact(
                OutputTypedArtifact::new("artifact.evidence", "evidence", "available")
                    .with_field("kind", "test"),
            )
            .with_artifact_ref(
                OutputTypedRef::new("artifact.ref", "file", "available").with_uri("artifact.json"),
            )
            .with_certificate(OutputTypedRef::new(
                "certificate.execution",
                "execution_certificate",
                "available",
            ))
            .with_policy_field("fallback_execution_allowed", "false")
            .with_lifecycle_field("phase", "report_only")
            .with_capability_snapshot_field("scope", "status")
            .to_json();

        assert!(json.contains("\"schema_version\":\"shardloom.output.v2\""));
        assert!(json.contains("\"result\":{\"fields\":[{\"key\":\"engine\""));
        assert!(json.contains("\"result_refs\":[{\"id\":\"result.local\""));
        assert!(json.contains("\"artifacts\":[{\"artifact_id\":\"artifact.evidence\""));
        assert!(json.contains("\"artifact_refs\":[{\"id\":\"artifact.ref\""));
        assert!(json.contains("\"certificates\":[{\"id\":\"certificate.execution\""));
        assert!(json.contains("\"policy\":{\"fields\":[{\"key\":\"fallback_execution_allowed\""));
        assert!(json.contains("\"lifecycle\":{\"fields\":[{\"key\":\"phase\""));
        assert!(json.contains("\"capability_snapshot\":{\"fields\":[{\"key\":\"scope\""));
        assert!(json.contains("\"fields\":[{\"key\":\"engine\""));
    }
    #[test]
    fn with_field_mirrors_into_typed_result_payload() {
        let envelope = OutputEnvelope::success("status", "ok", "ok").with_field("key", "value");

        assert_eq!(
            envelope.fields,
            vec![("key".to_string(), "value".to_string())]
        );
        assert_eq!(
            envelope.result.fields,
            vec![("key".to_string(), "value".to_string())]
        );
    }

    #[test]
    fn typed_policy_field_can_preserve_legacy_mirror_without_result_payload() {
        let envelope = OutputEnvelope::success("status", "ok", "ok")
            .with_policy_field("fallback_execution_allowed", "false")
            .with_legacy_field("fallback_execution_allowed", "false");

        assert!(envelope.result.fields.is_empty());
        assert_eq!(
            envelope.policy.fields,
            vec![(
                "fallback_execution_allowed".to_string(),
                "false".to_string()
            )]
        );
        assert_eq!(
            envelope.fields,
            vec![(
                "fallback_execution_allowed".to_string(),
                "false".to_string()
            )]
        );
    }

    #[test]
    fn render_text_returns_text() {
        assert_eq!(
            OutputEnvelope::success("status", "ok", "hello").render(OutputFormat::Text),
            "hello"
        );
    }
    #[test]
    fn render_json_looks_like_json() {
        assert!(
            OutputEnvelope::success("status", "ok", "hello")
                .render(OutputFormat::Json)
                .starts_with('{')
        );
    }
    #[test]
    fn has_errors_true_for_error_diagnostic() {
        let envelope = OutputEnvelope::success("status", "ok", "ok").with_diagnostic(
            Diagnostic::unsupported(DiagnosticCode::UnsupportedSql, "sql", "unsupported", None),
        );
        assert!(envelope.has_errors());
    }

    #[test]
    fn from_diagnostic_unsupported_feature_sets_unsupported_status() {
        let diagnostic =
            Diagnostic::unsupported(DiagnosticCode::UnsupportedSql, "sql", "unsupported", None);
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "unsupported", "unsupported", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Unsupported);
    }

    #[test]
    fn from_diagnostic_no_fallback_policy_sets_unsupported_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::NoFallbackPolicy,
            "fallback disabled",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic(
            "scan-plan",
            "fallback disabled",
            "fallback disabled",
            diagnostic,
        );
        assert_eq!(envelope.status, CommandStatus::Unsupported);
    }

    #[test]
    fn from_diagnostic_error_severity_sets_error_status_for_supported_category() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::ConfigurationError,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::Configuration,
            "config error",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "config", "config", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Error);
    }

    #[test]
    fn from_diagnostic_fatal_severity_sets_error_status_for_supported_category() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Fatal,
            crate::DiagnosticCategory::Execution,
            "execution failed",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "failed", "failed", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Error);
    }

    #[test]
    fn from_diagnostic_warning_severity_sets_warning_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::MetadataLoss,
            DiagnosticSeverity::Warning,
            crate::DiagnosticCategory::MetadataLoss,
            "metadata loss",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "warning", "warning", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Warning);
    }

    #[test]
    fn from_diagnostic_info_severity_sets_success_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::MissingStatistics,
            DiagnosticSeverity::Info,
            crate::DiagnosticCategory::Planning,
            "planning",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "ok", "ok", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Success);
    }

    #[test]
    fn from_error_invalid_operation_normalizes_invalid_input_and_fallback_disabled() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert_eq!(envelope.status, CommandStatus::Error);
        let diagnostic = &envelope.diagnostics[0];
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, crate::DiagnosticCategory::InvalidInput);
        assert!(!envelope.fallback.attempted);
        assert!(!envelope.fallback.allowed);
    }

    #[test]
    fn has_errors_true_for_unsupported_status_even_with_info_diagnostic() {
        let envelope = OutputEnvelope::new(
            "status",
            CommandStatus::Unsupported,
            "unsupported",
            "unsupported",
        )
        .with_diagnostic(Diagnostic::new(
            DiagnosticCode::MissingStatistics,
            DiagnosticSeverity::Info,
            crate::DiagnosticCategory::Planning,
            "info",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(envelope.has_errors());
    }

    #[test]
    fn to_json_includes_status_field() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"status\"")
        );
    }

    #[test]
    fn from_diagnostic_includes_diagnostic() {
        let diagnostic = Diagnostic::invalid_input("dataset_uri", "invalid", "fix");
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "bad input", "bad input", diagnostic);
        assert_eq!(envelope.diagnostics.len(), 1);
    }
    #[test]
    fn from_diagnostic_copies_fallback_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::UnsupportedSql,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::UnsupportedFeature,
            "unsupported",
            None,
            None,
            None,
            FallbackStatus {
                attempted: true,
                allowed: true,
                engine: Some("legacy".to_string()),
                reason: "test".to_string(),
            },
        );
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "bad input", "bad input", diagnostic);
        assert!(envelope.fallback.allowed);
        assert!(envelope.fallback.attempted);
    }

    #[test]
    fn from_error_includes_diagnostic() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert_eq!(envelope.diagnostics.len(), 1);
    }

    #[test]
    fn from_error_json_has_fallback_attempted_false() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert!(envelope.to_json().contains("\"attempted\":false"));
    }

    #[test]
    fn cli_api_json_protocol_is_report_only() {
        let report = CliApiJsonProtocolReport::contract_only();
        assert_eq!(report.output_envelope_schema_version, "shardloom.output.v2");
        assert!(report.required_envelope_fields.contains(&"fallback"));
        assert!(report.required_envelope_fields.contains(&"result"));
        assert!(report.required_envelope_fields.contains(&"certificates"));
        assert!(report.required_diagnostic_fields.contains(&"code"));
        assert_eq!(report.required_typed_payload_fields, vec!["fields"]);
        assert!(report.legacy_fields_mirror_present);
        assert!(!report.flat_fields_primary_payload_allowed);
        assert_eq!(report.compatibility_lock_status, "locked");
        assert!(
            report
                .compatibility_lock_fixture_statuses
                .contains(&"certified_local_execution")
        );
        assert!(report.json_error_paths_enveloped);
        assert!(report.unknown_command_json_enveloped);
        assert!(report.missing_binary_error_payload_shaped);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.external_publish);
        assert!(!report.has_errors());
    }

    #[test]
    fn cli_api_json_protocol_disallows_native_python_binding_claim() {
        let report = CliApiJsonProtocolReport::contract_only();
        assert_eq!(
            report.thin_python_wrapper_boundary,
            "cli_json_subprocess_first"
        );
        assert!(!report.pyo3_maturin_allowed);
        assert!(!report.dataframe_api_implemented);
    }

    #[test]
    fn python_wrapper_foundation_is_cli_json_only() {
        let report = PythonWrapperFoundationReport::contract_only();
        assert_eq!(report.transport_protocol_id, "shardloom.cli_json.v1");
        assert_eq!(report.invocation_model, "subprocess_cli_json");
        assert!(report.initial_command_scope.contains(&"api-compat-plan"));
        assert!(report.initial_command_scope.contains(&"vortex-run"));
        assert!(
            report
                .required_client_behaviors
                .contains(&"parse_output_envelope")
        );
        assert!(report.python_package_created);
        assert_eq!(report.package_status, "source_tree_wheel_sdist_ready");
        assert!(report.wheel_sdist_build_ready);
        assert!(report.fresh_environment_smoke_required);
        assert!(report.missing_binary_diagnostic_ready);
        assert!(report.conda_cli_package_required);
        assert!(report.conda_python_package_planned);
        assert!(report.conda_metapackage_planned);
        assert_eq!(report.conda_recipe_root, "packaging/conda");
        assert!(report.conda_cli_recipe_created);
        assert!(report.conda_python_recipe_created);
        assert!(report.conda_metapackage_recipe_created);
        assert!(report.benchmark_extras_optional);
        assert!(!report.pyo3_maturin_allowed);
        assert!(!report.native_extension_required);
        assert!(!report.has_errors());
    }

    #[test]
    fn python_wrapper_foundation_defers_mature_python_surfaces() {
        let report = PythonWrapperFoundationReport::contract_only();
        assert!(!report.dataframe_api_implemented);
        assert!(!report.notebook_api_implemented);
        assert!(!report.python_udf_runtime_implemented);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn python_wrapper_foundation_treats_probe_and_parser_execution_as_errors() {
        let mut report = PythonWrapperFoundationReport::contract_only();
        report.filesystem_probe = true;
        assert!(report.has_errors());

        let mut report = PythonWrapperFoundationReport::contract_only();
        report.network_probe = true;
        assert!(report.has_errors());

        let mut report = PythonWrapperFoundationReport::contract_only();
        report.catalog_probe = true;
        assert!(report.has_errors());

        let mut report = PythonWrapperFoundationReport::contract_only();
        report.adapter_probe = true;
        assert!(report.has_errors());

        let mut report = PythonWrapperFoundationReport::contract_only();
        report.parser_executed = true;
        assert!(report.has_errors());

        let mut report = PythonWrapperFoundationReport::contract_only();
        report.side_effect_free = false;
        assert!(report.has_errors());
    }
}
