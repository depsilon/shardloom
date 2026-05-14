//! Contract-first CG-23 REST/API planning surfaces.
//!
//! The reports in this module describe the remote control-plane contract
//! without starting a server, opening sockets, probing datasets, reading object
//! stores, consulting catalogs, executing plans, or enabling fallback engines.

use crate::output::EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION;
use crate::{
    CommandStatus, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, ShardLoomExecutionMode,
};

pub const REST_API_CONTRACT_SCHEMA_VERSION: &str = "shardloom.rest_api_contract.v1";
pub const REST_API_DISCOVERY_SCHEMA_VERSION: &str = "shardloom.rest_api_discovery_mode.v1";
pub const REST_API_PLAN_PREVIEW_SCHEMA_VERSION: &str = "shardloom.rest_api_plan_preview.v1";
pub const REST_API_LOCAL_LIFECYCLE_SCHEMA_VERSION: &str = "shardloom.rest_api_local_lifecycle.v1";
pub const REST_API_EVENT_STREAM_SCHEMA_VERSION: &str = "shardloom.rest_api_event_stream.v1";
pub const REST_API_SECURITY_GOVERNANCE_SCHEMA_VERSION: &str =
    "shardloom.rest_api_security_governance.v1";
pub const REST_API_DATA_PLANE_SCHEMA_VERSION: &str = "shardloom.rest_api_data_plane.v1";
pub const OPENAPI_CONTRACT_PATH: &str = "docs/api/shardloom-openapi-v1.yaml";
pub const ASYNCAPI_EVENT_CONTRACT_PATH: &str = "docs/api/shardloom-asyncapi-events-v1.yaml";
pub const OPENAPI_VERSION: &str = "3.2.0";
pub const ASYNCAPI_VERSION: &str = "3.0.0";
pub const API_VERSION: &str = "v1";
pub const PROBLEM_DETAILS_MEDIA_TYPE: &str = "application/problem+json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiMaturityStatus {
    AvailableContract,
    ContractDeclaredNoListener,
    Planned,
    BlockedUntilEvidence,
}

impl RestApiMaturityStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AvailableContract => "available_contract",
            Self::ContractDeclaredNoListener => "contract_declared_no_listener",
            Self::Planned => "planned",
            Self::BlockedUntilEvidence => "blocked_until_evidence",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiMaturityStage {
    pub stage_id: &'static str,
    pub label: &'static str,
    pub status: RestApiMaturityStatus,
    pub server_required: bool,
    pub execution_capable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiEndpointContract {
    pub method: &'static str,
    pub path: &'static str,
    pub resource: &'static str,
    pub maturity_stage: &'static str,
    pub side_effect_free: bool,
    pub execution_policy_required: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiContractReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub openapi_version: &'static str,
    pub openapi_contract_path: &'static str,
    pub problem_details_media_type: &'static str,
    pub represented_resources: Vec<&'static str>,
    pub execution_policy_fields: Vec<&'static str>,
    pub execution_mode_vocabulary: Vec<&'static str>,
    pub execution_mode_selection_schema_version: &'static str,
    pub execution_mode_selection_fields: Vec<&'static str>,
    pub execution_mode_response_fields: Vec<&'static str>,
    pub support_status: &'static str,
    pub unsupported_execution_mode_diagnostic_code: &'static str,
    pub unsupported_execution_mode_blocker_id: &'static str,
    pub unsupported_execution_mode_required_future_evidence: &'static str,
    pub result_policy_modes: Vec<&'static str>,
    pub maturity_stages: Vec<RestApiMaturityStage>,
    pub discovery_endpoints: Vec<RestApiEndpointContract>,
    pub contract_artifact_checked_in: bool,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub dataset_probe: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiDiscoveryModeReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub bind: String,
    pub mode: &'static str,
    pub contract_report: RestApiContractReport,
    pub health_endpoint: &'static str,
    pub version_endpoint: &'static str,
    pub capabilities_endpoint: &'static str,
    pub adapters_endpoint: &'static str,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub dataset_probe: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiPlanPreviewScenario {
    CertifiedLocalBatch,
    PartialHybridFixture,
    BlockedRemoteObjectStore,
    InvalidInput,
    UnsupportedOperator,
}

impl RestApiPlanPreviewScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedLocalBatch => "certified-local-batch",
            Self::PartialHybridFixture => "partial-hybrid-fixture",
            Self::BlockedRemoteObjectStore => "blocked-remote-object-store",
            Self::InvalidInput => "invalid-input",
            Self::UnsupportedOperator => "unsupported-operator",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::CertifiedLocalBatch,
            Self::PartialHybridFixture,
            Self::BlockedRemoteObjectStore,
            Self::InvalidInput,
            Self::UnsupportedOperator,
        ]
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "certified-local-batch" => Some(Self::CertifiedLocalBatch),
            "partial-hybrid-fixture" => Some(Self::PartialHybridFixture),
            "blocked-remote-object-store" => Some(Self::BlockedRemoteObjectStore),
            "invalid-input" => Some(Self::InvalidInput),
            "unsupported-operator" => Some(Self::UnsupportedOperator),
            _ => None,
        }
    }
}

type PlanPreviewStageSpec = (
    &'static str,
    RestApiPlanStageStatus,
    Option<&'static str>,
    &'static str,
);

#[allow(clippy::struct_excessive_bools)]
struct LocalLifecycleScenarioContract {
    lifecycle_status: RestApiLocalLifecycleStatus,
    query_id: &'static str,
    result_id: &'static str,
    result_ref: &'static str,
    result_artifact_ref: &'static str,
    lifecycle_events: Vec<RestApiLifecycleEvent>,
    non_certified_path_blocked: bool,
    cancellation_requested: bool,
    cancellation_status: &'static str,
    cancel_diagnostic_code: &'static str,
    retry_requested: bool,
    retry_status: &'static str,
    retry_diagnostic_code: &'static str,
    query_execution: bool,
    runtime_execution: bool,
    local_execution_performed: bool,
    diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
struct EventStreamScenarioContract {
    event_stream_status: RestApiEventStreamStatus,
    stream_id: &'static str,
    stream_ref: &'static str,
    engine_mode: &'static str,
    workload_ref: &'static str,
    progress_event_count: u32,
    state_event_count: u32,
    checkpoint_event_count: u32,
    watermark_event_count: u32,
    certificate_event_count: u32,
    lineage_event_count: u32,
    benchmark_event_count: u32,
    hot_cold_contribution_event_count: u32,
    live_fixture_certified: bool,
    hybrid_fixture_certified: bool,
    workload_certified: bool,
    cg22_workload_evidence_present: bool,
    cg8_runtime_evidence_present: bool,
    cg4_checkpoint_evidence_present: bool,
    cg16_execution_certificate_present: bool,
    production_claim_allowed: bool,
    broker_requested: bool,
    broker_required: bool,
    object_store_required: bool,
    freshness_certificate_ref: &'static str,
    state_certificate_ref: &'static str,
    continuous_view_certificate_ref: &'static str,
    delta_overlay_certificate_ref: &'static str,
    micro_segment_flush_evidence_ref: &'static str,
    hot_cold_contribution_report_ref: &'static str,
    execution_certificate_ref: &'static str,
    native_io_certificate_ref: &'static str,
    lineage_artifact_ref: &'static str,
    benchmark_event_ref: &'static str,
    diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
struct SecurityGovernanceScenarioContract {
    governance_status: RestApiSecurityGovernanceStatus,
    destructive_operation_requested: bool,
    destructive_policy_present: bool,
    agent_discovery_requested: bool,
    problem_details: Option<RestApiProblemDetailsPreview>,
    diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
struct DataPlaneScenarioContract {
    data_plane_status: RestApiDataPlaneStatus,
    flight_ticket_requested: bool,
    adbc_endpoint_requested: bool,
    standards_matrix_requested: bool,
    optional_transport_required: bool,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiPlanPreviewStatus {
    CertifiedPreview,
    PartialPreview,
    Blocked,
    InvalidInput,
    Unsupported,
}

impl RestApiPlanPreviewStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedPreview => "certified_preview",
            Self::PartialPreview => "partial_preview",
            Self::Blocked => "blocked",
            Self::InvalidInput => "invalid_input",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiPlanStageStatus {
    Ready,
    Certified,
    Partial,
    Blocked,
    InvalidInput,
    Unsupported,
    NotEvaluated,
}

impl RestApiPlanStageStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Certified => "certified",
            Self::Partial => "partial",
            Self::Blocked => "blocked",
            Self::InvalidInput => "invalid_input",
            Self::Unsupported => "unsupported",
            Self::NotEvaluated => "not_evaluated",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiPlanPreviewStage {
    pub stage_id: &'static str,
    pub label: &'static str,
    pub status: RestApiPlanStageStatus,
    pub diagnostic_code: Option<&'static str>,
    pub summary: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiProblemDetailsPreview {
    pub problem_type: &'static str,
    pub title: &'static str,
    pub http_status: u16,
    pub detail: &'static str,
    pub diagnostic_code: &'static str,
    pub unsupported_reason: Option<&'static str>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiPlanPreviewReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub scenario: RestApiPlanPreviewScenario,
    pub preview_status: RestApiPlanPreviewStatus,
    pub plan_handle: &'static str,
    pub endpoint_path: &'static str,
    pub endpoint_paths: Vec<&'static str>,
    pub preview_operations: Vec<&'static str>,
    pub execution_policy_fields: Vec<&'static str>,
    pub stages: Vec<RestApiPlanPreviewStage>,
    pub problem_details: Option<RestApiProblemDetailsPreview>,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub dataset_probe: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub parser_executed: bool,
    pub binder_executed: bool,
    pub native_logical_planned: bool,
    pub native_physical_planned: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub execution_delegated: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiLocalLifecycleScenario {
    CertifiedLocalBatch,
    CancelRequested,
    RetryRequested,
    BlockedUncertified,
}

impl RestApiLocalLifecycleScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedLocalBatch => "certified-local-batch",
            Self::CancelRequested => "cancel-requested",
            Self::RetryRequested => "retry-requested",
            Self::BlockedUncertified => "blocked-uncertified",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::CertifiedLocalBatch,
            Self::CancelRequested,
            Self::RetryRequested,
            Self::BlockedUncertified,
        ]
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "certified-local-batch" => Some(Self::CertifiedLocalBatch),
            "cancel-requested" => Some(Self::CancelRequested),
            "retry-requested" => Some(Self::RetryRequested),
            "blocked-uncertified" => Some(Self::BlockedUncertified),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiLocalLifecycleStatus {
    Succeeded,
    Canceled,
    RetryScheduled,
    Blocked,
}

impl RestApiLocalLifecycleStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Canceled => "canceled",
            Self::RetryScheduled => "retry_scheduled",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiEventStreamScenario {
    CertifiedLiveFixture,
    CertifiedHybridFixture,
    BlockedProductionWorkload,
    BrokerRequested,
}

impl RestApiEventStreamScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedLiveFixture => "certified-live-fixture",
            Self::CertifiedHybridFixture => "certified-hybrid-fixture",
            Self::BlockedProductionWorkload => "blocked-production-workload",
            Self::BrokerRequested => "broker-requested",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::CertifiedLiveFixture,
            Self::CertifiedHybridFixture,
            Self::BlockedProductionWorkload,
            Self::BrokerRequested,
        ]
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "certified-live-fixture" => Some(Self::CertifiedLiveFixture),
            "certified-hybrid-fixture" => Some(Self::CertifiedHybridFixture),
            "blocked-production-workload" => Some(Self::BlockedProductionWorkload),
            "broker-requested" => Some(Self::BrokerRequested),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiEventStreamStatus {
    CertifiedFixture,
    BlockedMissingEvidence,
    UnsupportedExternalBroker,
}

impl RestApiEventStreamStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedFixture => "certified_fixture",
            Self::BlockedMissingEvidence => "blocked_missing_evidence",
            Self::UnsupportedExternalBroker => "unsupported_external_broker",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiSecurityGovernanceScenario {
    SafeLocalDefault,
    DestructivePolicyRequired,
    AgentMcpDiscovery,
}

impl RestApiSecurityGovernanceScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SafeLocalDefault => "safe-local-default",
            Self::DestructivePolicyRequired => "destructive-policy-required",
            Self::AgentMcpDiscovery => "agent-mcp-discovery",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::SafeLocalDefault,
            Self::DestructivePolicyRequired,
            Self::AgentMcpDiscovery,
        ]
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "safe-local-default" => Some(Self::SafeLocalDefault),
            "destructive-policy-required" => Some(Self::DestructivePolicyRequired),
            "agent-mcp-discovery" => Some(Self::AgentMcpDiscovery),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiSecurityGovernanceStatus {
    AvailableContract,
    BlockedPolicyRequired,
    AgentDryRunOnly,
}

impl RestApiSecurityGovernanceStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AvailableContract => "available_contract",
            Self::BlockedPolicyRequired => "blocked_policy_required",
            Self::AgentDryRunOnly => "agent_dry_run_only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiDataPlaneScenario {
    ArtifactReferenceDefault,
    FlightTicketRequested,
    AdbcEndpointRequested,
    StandardsMatrix,
}

impl RestApiDataPlaneScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArtifactReferenceDefault => "artifact-reference-default",
            Self::FlightTicketRequested => "flight-ticket-requested",
            Self::AdbcEndpointRequested => "adbc-endpoint-requested",
            Self::StandardsMatrix => "standards-matrix",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::ArtifactReferenceDefault,
            Self::FlightTicketRequested,
            Self::AdbcEndpointRequested,
            Self::StandardsMatrix,
        ]
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "artifact-reference-default" => Some(Self::ArtifactReferenceDefault),
            "flight-ticket-requested" => Some(Self::FlightTicketRequested),
            "adbc-endpoint-requested" => Some(Self::AdbcEndpointRequested),
            "standards-matrix" => Some(Self::StandardsMatrix),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestApiDataPlaneStatus {
    ContractAvailable,
    OptionalTransportPlanned,
    StandardsMatrixAvailable,
}

impl RestApiDataPlaneStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContractAvailable => "contract_available",
            Self::OptionalTransportPlanned => "optional_transport_planned",
            Self::StandardsMatrixAvailable => "standards_matrix_available",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiLifecycleEvent {
    pub event_id: &'static str,
    pub status: &'static str,
    pub summary: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiResultPolicyContract {
    pub mode: &'static str,
    pub materialization: &'static str,
    pub certified_native: bool,
    pub preferred_for_high_fidelity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiEventStreamEventContract {
    pub event_type: &'static str,
    pub category: &'static str,
    pub cloudevents_type: &'static str,
    pub subject: &'static str,
    pub data_schema_ref: &'static str,
    pub evidence_ref: &'static str,
    pub certificate_ref: &'static str,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiAuthPostureContract {
    pub auth_kind: &'static str,
    pub status: &'static str,
    pub credential_ref: &'static str,
    pub credential_reference_only: bool,
    pub secret_material_allowed: bool,
    pub runtime_resolution_allowed: bool,
    pub local_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiScopeContract {
    pub scope: &'static str,
    pub default_access: &'static str,
    pub policy_required: bool,
    pub destructive: bool,
    pub audit_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiAuditPolicyContract {
    pub event_type: &'static str,
    pub action: &'static str,
    pub required: bool,
    pub redaction_required: bool,
    pub evidence_ref: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiMcpContract {
    pub name: &'static str,
    pub contract_kind: &'static str,
    pub default_operation: &'static str,
    pub dry_run_only: bool,
    pub effectful: bool,
    pub output_schema_ref: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiEvidenceModelSignal {
    pub signal: &'static str,
    pub standard: &'static str,
    pub schema_ref: &'static str,
    pub redaction_required: bool,
    pub certificate_ref_required: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiDataPlaneTransferContract {
    pub mode: &'static str,
    pub transport: &'static str,
    pub materialization: &'static str,
    pub fidelity: &'static str,
    pub result_policy: &'static str,
    pub preferred_for_large_payloads: bool,
    pub native_vortex_fidelity: bool,
    pub decoded_columnar_boundary: bool,
    pub optional_dependency_required: bool,
    pub enabled_by_default: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiStandardsBoundaryContract {
    pub standard: &'static str,
    pub category: &'static str,
    pub posture: &'static str,
    pub dependency_policy: &'static str,
    pub boundary_classification: &'static str,
    pub control_plane_role: &'static str,
    pub materialization: &'static str,
    pub external_compute_boundary: bool,
    pub broker_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub execution_allowed: bool,
    pub fallback_allowed: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiLocalLifecycleReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub scenario: RestApiLocalLifecycleScenario,
    pub lifecycle_status: RestApiLocalLifecycleStatus,
    pub endpoint_paths: Vec<&'static str>,
    pub lifecycle_operations: Vec<&'static str>,
    pub query_id: &'static str,
    pub plan_handle: &'static str,
    pub result_id: &'static str,
    pub result_ref: &'static str,
    pub result_artifact_ref: &'static str,
    pub execution_certificate_ref: &'static str,
    pub native_io_certificate_ref: &'static str,
    pub materialization_boundary_report_ref: &'static str,
    pub profile_artifact_ref: &'static str,
    pub lineage_artifact_ref: &'static str,
    pub no_fallback_evidence_artifact_ref: &'static str,
    pub lifecycle_events: Vec<RestApiLifecycleEvent>,
    pub result_policies: Vec<RestApiResultPolicyContract>,
    pub inline_json_available: bool,
    pub paged_json_available: bool,
    pub jsonl_ndjson_available: bool,
    pub vortex_artifact_available: bool,
    pub object_reference_available: bool,
    pub arrow_ipc_available: bool,
    pub arrow_ipc_materialization: &'static str,
    pub arrow_ipc_certified_native: bool,
    pub preferred_high_fidelity_result_modes: Vec<&'static str>,
    pub result_ttl_seconds: u32,
    pub retention_policy: &'static str,
    pub cleanup_required: bool,
    pub cleanup_endpoint: &'static str,
    pub non_certified_path_blocked: bool,
    pub cancellation_requested: bool,
    pub cancellation_status: &'static str,
    pub cancel_diagnostic_code: &'static str,
    pub retry_requested: bool,
    pub retry_status: &'static str,
    pub retry_diagnostic_code: &'static str,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub dataset_probe: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub local_execution_performed: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub execution_delegated: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiEventStreamReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub scenario: RestApiEventStreamScenario,
    pub event_stream_status: RestApiEventStreamStatus,
    pub endpoint_paths: Vec<&'static str>,
    pub event_operations: Vec<&'static str>,
    pub delivery_protocols: Vec<&'static str>,
    pub stream_id: &'static str,
    pub stream_ref: &'static str,
    pub engine_mode: &'static str,
    pub workload_ref: &'static str,
    pub openapi_contract_path: &'static str,
    pub asyncapi_version: &'static str,
    pub asyncapi_contract_path: &'static str,
    pub cloudevents_spec_version: &'static str,
    pub cloudevents_required_fields: Vec<&'static str>,
    pub event_contracts: Vec<RestApiEventStreamEventContract>,
    pub sse_first: bool,
    pub sse_media_type: &'static str,
    pub websocket_supported: bool,
    pub websocket_required: bool,
    pub bidirectional_interaction_required: bool,
    pub event_count: u32,
    pub progress_event_count: u32,
    pub state_event_count: u32,
    pub checkpoint_event_count: u32,
    pub watermark_event_count: u32,
    pub certificate_event_count: u32,
    pub lineage_event_count: u32,
    pub benchmark_event_count: u32,
    pub hot_cold_contribution_event_count: u32,
    pub live_fixture_certified: bool,
    pub hybrid_fixture_certified: bool,
    pub workload_certified: bool,
    pub cg22_workload_evidence_present: bool,
    pub cg8_runtime_evidence_present: bool,
    pub cg4_checkpoint_evidence_present: bool,
    pub cg16_execution_certificate_present: bool,
    pub production_claim_allowed: bool,
    pub broker_requested: bool,
    pub broker_required: bool,
    pub object_store_required: bool,
    pub freshness_certificate_ref: &'static str,
    pub state_certificate_ref: &'static str,
    pub continuous_view_certificate_ref: &'static str,
    pub delta_overlay_certificate_ref: &'static str,
    pub micro_segment_flush_evidence_ref: &'static str,
    pub hot_cold_contribution_report_ref: &'static str,
    pub execution_certificate_ref: &'static str,
    pub native_io_certificate_ref: &'static str,
    pub lineage_artifact_ref: &'static str,
    pub benchmark_event_ref: &'static str,
    pub no_fallback_evidence_artifact_ref: &'static str,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub broker_io: bool,
    pub object_store_io: bool,
    pub dataset_probe: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub execution_delegated: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiSecurityGovernanceReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub scenario: RestApiSecurityGovernanceScenario,
    pub governance_status: RestApiSecurityGovernanceStatus,
    pub endpoint_paths: Vec<&'static str>,
    pub governance_operations: Vec<&'static str>,
    pub auth_postures: Vec<RestApiAuthPostureContract>,
    pub scopes: Vec<RestApiScopeContract>,
    pub audit_policies: Vec<RestApiAuditPolicyContract>,
    pub mcp_resources: Vec<RestApiMcpContract>,
    pub mcp_tools: Vec<RestApiMcpContract>,
    pub evidence_model: Vec<RestApiEvidenceModelSignal>,
    pub openapi_contract_path: &'static str,
    pub problem_details_media_type: &'static str,
    pub problem_details: Option<RestApiProblemDetailsPreview>,
    pub local_only_default: bool,
    pub credential_references_only: bool,
    pub credentials_resolved: bool,
    pub token_secret_ref: &'static str,
    pub mtls_certificate_ref: &'static str,
    pub oidc_issuer_ref: &'static str,
    pub service_account_ref: &'static str,
    pub raw_secret_values_present: bool,
    pub secrets_redacted: bool,
    pub redaction_policy: &'static str,
    pub destructive_operation_requested: bool,
    pub destructive_policy_required: bool,
    pub destructive_policy_present: bool,
    pub destructive_operations_allowed: bool,
    pub audit_required: bool,
    pub audit_evidence_ref: &'static str,
    pub mcp_dry_run_default: bool,
    pub mcp_effectful_tools_allowed: bool,
    pub mcp_discovery_side_effect_free: bool,
    pub opentelemetry_exporter_enabled: bool,
    pub runtime_collection_enabled: bool,
    pub openlineage_facets_mapped: bool,
    pub problem_details_mapped: bool,
    pub cloudevents_mapped: bool,
    pub certificate_refs_mapped: bool,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub dataset_probe: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub credential_resolution: bool,
    pub secret_resolution: bool,
    pub raw_secret_emitted: bool,
    pub audit_write_io: bool,
    pub mcp_tool_execution: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub execution_delegated: bool,
    pub diagnostics: Vec<Diagnostic>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApiDataPlaneReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub api_version: &'static str,
    pub scenario: RestApiDataPlaneScenario,
    pub data_plane_status: RestApiDataPlaneStatus,
    pub endpoint_paths: Vec<&'static str>,
    pub data_plane_operations: Vec<&'static str>,
    pub transfer_contracts: Vec<RestApiDataPlaneTransferContract>,
    pub standards: Vec<RestApiStandardsBoundaryContract>,
    pub openapi_contract_path: &'static str,
    pub rest_control_plane_required: bool,
    pub rest_control_plane_sufficient_for_local_use: bool,
    pub flight_adbc_required_for_basic_local_use: bool,
    pub flight_ticket_requested: bool,
    pub flight_ticket_supported: bool,
    pub adbc_endpoint_requested: bool,
    pub adbc_endpoint_supported: bool,
    pub optional_transport_required: bool,
    pub large_payload_threshold_bytes: u64,
    pub preferred_large_payload_modes: Vec<&'static str>,
    pub inline_json_max_bytes: u64,
    pub paged_json_available: bool,
    pub jsonl_ndjson_available: bool,
    pub vortex_artifact_available: bool,
    pub object_reference_available: bool,
    pub arrow_ipc_decoded_boundary_available: bool,
    pub arrow_ipc_certified_native: bool,
    pub decoded_columnar_boundary_declared: bool,
    pub materialization_declared: bool,
    pub fidelity_declared: bool,
    pub result_policy_declared: bool,
    pub no_fallback_evidence_artifact_ref: &'static str,
    pub security_governance_policy_ref: &'static str,
    pub standards_matrix_requested: bool,
    pub standards_matrix_count: usize,
    pub server_started: bool,
    pub network_listener_opened: bool,
    pub flight_server_started: bool,
    pub adbc_endpoint_opened: bool,
    pub broker_io: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub dataset_probe: bool,
    pub credential_resolution: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub query_execution: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub execution_delegated: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl RestApiContractReport {
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: REST_API_CONTRACT_SCHEMA_VERSION,
            report_id: "cg23.rest_api_contract.discovery",
            api_version: API_VERSION,
            openapi_version: OPENAPI_VERSION,
            openapi_contract_path: OPENAPI_CONTRACT_PATH,
            problem_details_media_type: PROBLEM_DETAILS_MEDIA_TYPE,
            represented_resources: vec![
                "health",
                "version",
                "capabilities",
                "adapters",
                "sources",
                "sinks",
                "plans",
                "queries",
                "results",
                "events",
                "certificates",
                "profiles",
                "benchmarks",
                "migration",
                "lineage",
                "governance",
                "security",
                "observability",
                "agents",
                "data-plane",
                "standards",
            ],
            execution_policy_fields: vec![
                "requested_execution_mode",
                "engine_mode",
                "fallback_policy",
                "materialization_policy",
                "result_policy",
                "evidence_policy",
            ],
            execution_mode_vocabulary: rest_execution_mode_vocabulary(),
            execution_mode_selection_schema_version: EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION,
            execution_mode_selection_fields: rest_execution_mode_selection_fields(),
            execution_mode_response_fields: vec![
                "execution_mode_selection",
                "fallback",
                "diagnostics",
                "fields",
            ],
            support_status: "report_only",
            unsupported_execution_mode_diagnostic_code: "SL_UNSUPPORTED_EXECUTION_MODE",
            unsupported_execution_mode_blocker_id: "GAR-FLOW-3A",
            unsupported_execution_mode_required_future_evidence: "REST runtime/server admission and execution-mode request handling evidence",
            result_policy_modes: vec![
                "inline_json",
                "paged_json",
                "jsonl_ndjson",
                "arrow_ipc_decoded_boundary",
                "vortex_artifact",
                "object_reference",
                "flight_ticket_future",
                "adbc_endpoint_future",
            ],
            maturity_stages: rest_api_maturity_stages(),
            discovery_endpoints: discovery_endpoint_contracts(),
            contract_artifact_checked_in: true,
            server_started: false,
            network_listener_opened: false,
            dataset_probe: false,
            object_store_io: false,
            catalog_probe: false,
            credential_resolution: false,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            external_engine_invoked: false,
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
        !self.contract_artifact_checked_in
            || self.server_started
            || self.network_listener_opened
            || self.dataset_probe
            || self.object_store_io
            || self.catalog_probe
            || self.credential_resolution
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || !self.discovery_endpoints_side_effect_free()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn discovery_endpoints_side_effect_free(&self) -> bool {
        self.discovery_endpoints
            .iter()
            .all(|endpoint| endpoint.side_effect_free && !endpoint.execution_policy_required)
    }

    #[must_use]
    pub fn endpoint_paths(&self) -> Vec<&'static str> {
        self.discovery_endpoints
            .iter()
            .map(|endpoint| endpoint.path)
            .collect()
    }

    #[must_use]
    pub fn maturity_stage_summary(&self) -> String {
        self.maturity_stages
            .iter()
            .map(|stage| format!("{}:{}", stage.stage_id, stage.status.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api contract\nschema_version: {}\nreport: {}\napi version: {}\nopenapi: {}\ncontract: {}\nresources: {}\ndiscovery endpoints: {}\nexecution modes: {}\nexecution-mode support: {}\nproblem details: {}\nserver started: false\nnetwork listener: false\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.api_version,
            self.openapi_version,
            self.openapi_contract_path,
            self.represented_resources.join(", "),
            self.endpoint_paths().join(", "),
            self.execution_mode_vocabulary.join(", "),
            self.support_status,
            self.problem_details_media_type,
        )
    }
}

fn rest_execution_mode_vocabulary() -> Vec<&'static str> {
    vec![
        ShardLoomExecutionMode::Auto.as_str(),
        ShardLoomExecutionMode::CompatibilityImportCertified.as_str(),
        ShardLoomExecutionMode::PreparedVortex.as_str(),
        ShardLoomExecutionMode::NativeVortex.as_str(),
        ShardLoomExecutionMode::DirectCompatibilityTransient.as_str(),
    ]
}

fn rest_execution_mode_selection_fields() -> Vec<&'static str> {
    vec![
        "execution_mode_selection_schema_version",
        "requested_execution_mode",
        "selected_execution_mode",
        "execution_mode",
        "mode_selection_reason",
        "execution_mode_family",
        "source_format",
        "workload_constitution_id",
        "compatibility_import_included",
        "vortex_prepare_included",
        "vortex_write_reopen_included",
        "direct_transient_execution",
        "vortex_native_claim_allowed",
        "certification_requested",
        "result_sink_requested",
        "prepared_artifact_available",
        "native_vortex_provider_available",
        "mode_supported",
        "support_status",
        "unsupported_diagnostic_code",
        "blocker_id",
        "required_future_evidence",
        "claim_gate_status",
        "claim_gate_reason",
        "fallback_attempted",
        "external_engine_invoked",
    ]
}

impl RestApiDiscoveryModeReport {
    #[must_use]
    pub fn contract_only(bind: impl Into<String>) -> Self {
        let contract_report = RestApiContractReport::contract_only();
        Self {
            schema_version: REST_API_DISCOVERY_SCHEMA_VERSION,
            report_id: "cg23.rest_api_discovery_mode.contract",
            bind: bind.into(),
            mode: "discovery",
            health_endpoint: "/v1/health",
            version_endpoint: "/v1/version",
            capabilities_endpoint: "/v1/capabilities",
            adapters_endpoint: "/v1/adapters",
            server_started: false,
            network_listener_opened: false,
            dataset_probe: false,
            object_store_io: false,
            catalog_probe: false,
            credential_resolution: false,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
            contract_report,
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
        self.mode != "discovery"
            || self.server_started
            || self.network_listener_opened
            || self.dataset_probe
            || self.object_store_io
            || self.catalog_probe
            || self.credential_resolution
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.contract_report.has_errors()
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
            "rest discovery mode contract\nschema_version: {}\nreport: {}\nmode: {}\nbind: {}\nhealth: {}\nversion: {}\ncapabilities: {}\nadapters: {}\nserver started: false\nnetwork listener: false\ndataset probe: false\nquery execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.mode,
            self.bind,
            self.health_endpoint,
            self.version_endpoint,
            self.capabilities_endpoint,
            self.adapters_endpoint,
        )
    }
}

impl RestApiPlanPreviewReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn for_scenario(scenario: RestApiPlanPreviewScenario) -> Self {
        let (preview_status, plan_handle, stages, problem_details, diagnostics) = match scenario {
            RestApiPlanPreviewScenario::CertifiedLocalBatch => (
                RestApiPlanPreviewStatus::CertifiedPreview,
                "plan://cg23/certified-local-batch",
                plan_preview_stage_bundle(&[
                    ("parser", RestApiPlanStageStatus::Ready, None, "request shape parsed"),
                    ("binder", RestApiPlanStageStatus::Ready, None, "logical symbols bound"),
                    (
                        "native_logical",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "native logical plan available",
                    ),
                    (
                        "native_physical",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "native physical plan available",
                    ),
                    (
                        "execution_readiness",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "certified local batch path is ready",
                    ),
                    (
                        "evidence_readiness",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "correctness and native I/O evidence handles are present",
                    ),
                    (
                        "certification",
                        RestApiPlanStageStatus::Certified,
                        None,
                        "preview can expose certification handles",
                    ),
                ]),
                None,
                Vec::new(),
            ),
            RestApiPlanPreviewScenario::PartialHybridFixture => (
                RestApiPlanPreviewStatus::PartialPreview,
                "plan://cg23/partial-hybrid-fixture",
                plan_preview_stage_bundle(&[
                    ("parser", RestApiPlanStageStatus::Ready, None, "request shape parsed"),
                    ("binder", RestApiPlanStageStatus::Ready, None, "logical symbols bound"),
                    (
                        "native_logical",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "hybrid logical overlay is inspectable",
                    ),
                    (
                        "native_physical",
                        RestApiPlanStageStatus::Partial,
                        None,
                        "fixture physical plan is inspectable; production serving is not certified",
                    ),
                    (
                        "execution_readiness",
                        RestApiPlanStageStatus::Partial,
                        None,
                        "fixture readiness is available without production API execution",
                    ),
                    (
                        "evidence_readiness",
                        RestApiPlanStageStatus::Partial,
                        None,
                        "fixture evidence is present but production workload evidence is incomplete",
                    ),
                    (
                        "certification",
                        RestApiPlanStageStatus::Partial,
                        None,
                        "certification preview is partial",
                    ),
                ]),
                None,
                vec![Diagnostic::new(
                    DiagnosticCode::NotImplemented,
                    DiagnosticSeverity::Warning,
                    DiagnosticCategory::Planning,
                    "REST plan preview is partial for hybrid fixture workloads.",
                    Some("rest_api_plan_preview".to_string()),
                    Some(
                        "Hybrid fixture evidence exists, but production remote API serving remains blocked."
                            .to_string(),
                    ),
                    Some(
                        "Use local hybrid-overlay-run evidence for fixture inspection until API-A7 is certified."
                            .to_string(),
                    ),
                    FallbackStatus::disabled_by_policy(),
                )],
            ),
            RestApiPlanPreviewScenario::BlockedRemoteObjectStore => (
                RestApiPlanPreviewStatus::Blocked,
                "plan://cg23/blocked-remote-object-store",
                plan_preview_stage_bundle(&[
                    ("parser", RestApiPlanStageStatus::Ready, None, "request shape parsed"),
                    ("binder", RestApiPlanStageStatus::Ready, None, "logical symbols bound"),
                    (
                        "native_logical",
                        RestApiPlanStageStatus::Ready,
                        None,
                        "logical plan is inspectable",
                    ),
                    (
                        "native_physical",
                        RestApiPlanStageStatus::Blocked,
                        Some("SL_OBJECT_STORE_UNSUPPORTED"),
                        "remote object-store access is not certified for REST execution",
                    ),
                    (
                        "execution_readiness",
                        RestApiPlanStageStatus::Blocked,
                        Some("SL_OBJECT_STORE_UNSUPPORTED"),
                        "execution is blocked before any source probe",
                    ),
                    (
                        "evidence_readiness",
                        RestApiPlanStageStatus::Blocked,
                        Some("SL_OBJECT_STORE_UNSUPPORTED"),
                        "required native I/O evidence is missing",
                    ),
                    (
                        "certification",
                        RestApiPlanStageStatus::Blocked,
                        Some("SL_OBJECT_STORE_UNSUPPORTED"),
                        "certification preview reports blockers only",
                    ),
                ]),
                Some(RestApiProblemDetailsPreview {
                    problem_type: "https://shardloom.dev/problems/object-store-unsupported",
                    title: "Remote object-store source is not certified",
                    http_status: 422,
                    detail:
                        "The plan references a remote object-store source whose REST execution path is not certified.",
                    diagnostic_code: "SL_OBJECT_STORE_UNSUPPORTED",
                    unsupported_reason: Some("remote object-store data access is blocked before probing"),
                }),
                vec![Diagnostic::unsupported(
                    DiagnosticCode::ObjectStoreUnsupported,
                    "rest_api_remote_object_store_plan",
                    "Remote object-store REST plan preview is blocked before data access.",
                    Some("Use a certified local Vortex fixture or wait for native object-store certificates.".to_string()),
                )],
            ),
            RestApiPlanPreviewScenario::InvalidInput => (
                RestApiPlanPreviewStatus::InvalidInput,
                "plan://cg23/invalid-input",
                plan_preview_stage_bundle(&[
                    (
                        "parser",
                        RestApiPlanStageStatus::InvalidInput,
                        Some("SL_INVALID_INPUT"),
                        "request cannot be parsed as a plan preview",
                    ),
                    (
                        "binder",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "binding skipped after parser failure",
                    ),
                    (
                        "native_logical",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "logical planning skipped after parser failure",
                    ),
                    (
                        "native_physical",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "physical planning skipped after parser failure",
                    ),
                    (
                        "execution_readiness",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "readiness skipped after parser failure",
                    ),
                    (
                        "evidence_readiness",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "evidence readiness skipped after parser failure",
                    ),
                    (
                        "certification",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_INVALID_INPUT"),
                        "certification skipped after parser failure",
                    ),
                ]),
                Some(RestApiProblemDetailsPreview {
                    problem_type: "https://shardloom.dev/problems/invalid-plan-preview-request",
                    title: "Invalid plan preview request",
                    http_status: 422,
                    detail: "The request body is missing a valid plan object and execution policy.",
                    diagnostic_code: "SL_INVALID_INPUT",
                    unsupported_reason: None,
                }),
                vec![Diagnostic::invalid_input(
                    "rest_api_plan_preview_request",
                    "request body is missing a valid plan object and execution policy",
                    "Submit a PlanPreviewRequest with plan and policy fields.",
                )],
            ),
            RestApiPlanPreviewScenario::UnsupportedOperator => (
                RestApiPlanPreviewStatus::Unsupported,
                "plan://cg23/unsupported-operator",
                plan_preview_stage_bundle(&[
                    ("parser", RestApiPlanStageStatus::Ready, None, "request shape parsed"),
                    ("binder", RestApiPlanStageStatus::Ready, None, "logical symbols bound"),
                    (
                        "native_logical",
                        RestApiPlanStageStatus::Unsupported,
                        Some("SL_UNSUPPORTED_SQL"),
                        "operator is not supported by the native logical planner",
                    ),
                    (
                        "native_physical",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_UNSUPPORTED_SQL"),
                        "physical planning skipped after unsupported logical operator",
                    ),
                    (
                        "execution_readiness",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_UNSUPPORTED_SQL"),
                        "execution readiness skipped after unsupported logical operator",
                    ),
                    (
                        "evidence_readiness",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_UNSUPPORTED_SQL"),
                        "evidence readiness skipped after unsupported logical operator",
                    ),
                    (
                        "certification",
                        RestApiPlanStageStatus::NotEvaluated,
                        Some("SL_UNSUPPORTED_SQL"),
                        "certification skipped after unsupported logical operator",
                    ),
                ]),
                Some(RestApiProblemDetailsPreview {
                    problem_type: "https://shardloom.dev/problems/unsupported-plan-operator",
                    title: "Unsupported plan operator",
                    http_status: 422,
                    detail:
                        "The plan contains an operator that has no certified native ShardLoom path.",
                    diagnostic_code: "SL_UNSUPPORTED_SQL",
                    unsupported_reason: Some("unsupported operator rejected without fallback execution"),
                }),
                vec![Diagnostic::unsupported(
                    DiagnosticCode::UnsupportedSql,
                    "rest_api_plan_operator",
                    "Plan preview rejected an unsupported operator without delegating execution.",
                    Some("Rewrite the plan to certified native operators or inspect capability reports.".to_string()),
                )],
            ),
        };

        let native_logical_planned = plan_preview_stage_planned(&stages, "native_logical");
        let native_physical_planned = plan_preview_stage_planned(&stages, "native_physical");

        Self {
            schema_version: REST_API_PLAN_PREVIEW_SCHEMA_VERSION,
            report_id: "cg23.rest_api_plan_preview",
            api_version: API_VERSION,
            scenario,
            preview_status,
            plan_handle,
            endpoint_path: "/v1/plans",
            endpoint_paths: plan_preview_endpoint_paths(),
            preview_operations: vec![
                "plan_handle",
                "validate",
                "explain",
                "estimate",
                "unsupported_report",
                "certification_preview",
            ],
            execution_policy_fields: vec![
                "requested_execution_mode",
                "engine_mode",
                "fallback_policy",
                "materialization_policy",
                "result_policy",
                "evidence_policy",
            ],
            stages,
            problem_details,
            server_started: false,
            network_listener_opened: false,
            dataset_probe: false,
            object_store_io: false,
            catalog_probe: false,
            credential_resolution: false,
            parser_executed: true,
            binder_executed: !matches!(scenario, RestApiPlanPreviewScenario::InvalidInput),
            native_logical_planned,
            native_physical_planned,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            execution_delegated: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.effect_policy_violated() {
            CommandStatus::Error
        } else {
            match self.preview_status {
                RestApiPlanPreviewStatus::CertifiedPreview => CommandStatus::Success,
                RestApiPlanPreviewStatus::PartialPreview | RestApiPlanPreviewStatus::Blocked => {
                    CommandStatus::Warning
                }
                RestApiPlanPreviewStatus::InvalidInput => CommandStatus::Error,
                RestApiPlanPreviewStatus::Unsupported => CommandStatus::Unsupported,
            }
        }
    }

    #[must_use]
    pub fn effect_policy_violated(&self) -> bool {
        self.server_started
            || self.network_listener_opened
            || self.dataset_probe
            || self.object_store_io
            || self.catalog_probe
            || self.credential_resolution
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.execution_delegated
    }

    #[must_use]
    pub fn stage_order(&self) -> Vec<&'static str> {
        self.stages.iter().map(|stage| stage.stage_id).collect()
    }

    #[must_use]
    pub fn stage_status_summary(&self) -> String {
        self.stages
            .iter()
            .map(|stage| format!("{}:{}", stage.stage_id, stage.status.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn stage_diagnostic_summary(&self) -> String {
        let values = self
            .stages
            .iter()
            .filter_map(|stage| {
                stage
                    .diagnostic_code
                    .map(|code| format!("{}:{code}", stage.stage_id))
            })
            .collect::<Vec<_>>();
        if values.is_empty() {
            "none".to_string()
        } else {
            values.join(",")
        }
    }

    #[must_use]
    pub fn problem_details_emitted(&self) -> bool {
        self.problem_details.is_some()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api plan preview\nschema_version: {}\nreport: {}\nscenario: {}\nstatus: {}\nplan handle: {}\nendpoints: {}\nstages: {}\nproblem details emitted: {}\nserver started: false\nnetwork listener: false\nquery execution: disabled\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scenario.as_str(),
            self.preview_status.as_str(),
            self.plan_handle,
            self.endpoint_paths.join(", "),
            self.stage_status_summary(),
            self.problem_details_emitted(),
        )
    }
}

impl RestApiLocalLifecycleReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn for_scenario(scenario: RestApiLocalLifecycleScenario) -> Self {
        let contract = lifecycle_scenario_contract(scenario);
        let evidence_available =
            matches!(scenario, RestApiLocalLifecycleScenario::CertifiedLocalBatch);

        Self {
            schema_version: REST_API_LOCAL_LIFECYCLE_SCHEMA_VERSION,
            report_id: "cg23.rest_api_local_lifecycle",
            api_version: API_VERSION,
            scenario,
            lifecycle_status: contract.lifecycle_status,
            endpoint_paths: local_lifecycle_endpoint_paths(),
            lifecycle_operations: vec![
                "execute",
                "status",
                "cancel",
                "retry",
                "profile",
                "certificates",
                "lineage",
                "results",
                "artifacts",
                "cleanup",
            ],
            query_id: contract.query_id,
            plan_handle: local_lifecycle_plan_handle(scenario),
            result_id: contract.result_id,
            result_ref: contract.result_ref,
            result_artifact_ref: contract.result_artifact_ref,
            execution_certificate_ref: if evidence_available {
                "certificates/cg23/certified-local-batch/execution.json"
            } else {
                "none"
            },
            native_io_certificate_ref: if evidence_available {
                "certificates/cg23/certified-local-batch/native-io.json"
            } else {
                "none"
            },
            materialization_boundary_report_ref: if evidence_available {
                "artifacts/cg23/certified-local-batch/materialization.json"
            } else {
                "none"
            },
            profile_artifact_ref: if evidence_available {
                "artifacts/cg23/certified-local-batch/profile.json"
            } else {
                "none"
            },
            lineage_artifact_ref: if evidence_available {
                "artifacts/cg23/certified-local-batch/lineage.json"
            } else {
                "none"
            },
            no_fallback_evidence_artifact_ref: if evidence_available {
                "artifacts/cg23/certified-local-batch/no-fallback.json"
            } else {
                "none"
            },
            lifecycle_events: contract.lifecycle_events,
            result_policies: local_lifecycle_result_policies(),
            inline_json_available: matches!(
                scenario,
                RestApiLocalLifecycleScenario::CertifiedLocalBatch
            ),
            paged_json_available: matches!(
                scenario,
                RestApiLocalLifecycleScenario::CertifiedLocalBatch
            ),
            jsonl_ndjson_available: matches!(
                scenario,
                RestApiLocalLifecycleScenario::CertifiedLocalBatch
            ),
            vortex_artifact_available: matches!(
                scenario,
                RestApiLocalLifecycleScenario::CertifiedLocalBatch
            ),
            object_reference_available: false,
            arrow_ipc_available: matches!(
                scenario,
                RestApiLocalLifecycleScenario::CertifiedLocalBatch
            ),
            arrow_ipc_materialization: "decoded_columnar_boundary",
            arrow_ipc_certified_native: false,
            preferred_high_fidelity_result_modes: vec!["vortex_artifact", "object_reference"],
            result_ttl_seconds: 3600,
            retention_policy: "local_ephemeral",
            cleanup_required: true,
            cleanup_endpoint: "/v1/results/{result_id}",
            non_certified_path_blocked: contract.non_certified_path_blocked,
            cancellation_requested: contract.cancellation_requested,
            cancellation_status: contract.cancellation_status,
            cancel_diagnostic_code: contract.cancel_diagnostic_code,
            retry_requested: contract.retry_requested,
            retry_status: contract.retry_status,
            retry_diagnostic_code: contract.retry_diagnostic_code,
            server_started: false,
            network_listener_opened: false,
            dataset_probe: false,
            object_store_io: false,
            catalog_probe: false,
            credential_resolution: false,
            data_read: false,
            data_materialized: false,
            query_execution: contract.query_execution,
            runtime_execution: contract.runtime_execution,
            local_execution_performed: contract.local_execution_performed,
            write_io: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            execution_delegated: false,
            diagnostics: contract.diagnostics,
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.effect_policy_violated() {
            CommandStatus::Error
        } else if matches!(self.lifecycle_status, RestApiLocalLifecycleStatus::Blocked) {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        }
    }

    #[must_use]
    pub fn effect_policy_violated(&self) -> bool {
        self.server_started
            || self.network_listener_opened
            || self.dataset_probe
            || self.object_store_io
            || self.catalog_probe
            || self.credential_resolution
            || self.data_read
            || self.data_materialized
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.execution_delegated
    }

    #[must_use]
    pub fn lifecycle_event_summary(&self) -> String {
        self.lifecycle_events
            .iter()
            .map(|event| format!("{}:{}", event.event_id, event.status))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn result_policy_summary(&self) -> String {
        self.result_policies
            .iter()
            .map(|policy| format!("{}:{}", policy.mode, policy.materialization))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api local lifecycle\nschema_version: {}\nreport: {}\nscenario: {}\nstatus: {}\nquery: {}\nresult: {}\nevents: {}\nresult policies: {}\nserver started: false\nnetwork listener: false\nexternal engine: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scenario.as_str(),
            self.lifecycle_status.as_str(),
            self.query_id,
            self.result_ref,
            self.lifecycle_event_summary(),
            self.result_policy_summary(),
        )
    }
}

impl RestApiEventStreamReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn for_scenario(scenario: RestApiEventStreamScenario) -> Self {
        let contract = event_stream_scenario_contract(scenario);

        Self {
            schema_version: REST_API_EVENT_STREAM_SCHEMA_VERSION,
            report_id: "cg23.rest_api_event_stream",
            api_version: API_VERSION,
            scenario,
            event_stream_status: contract.event_stream_status,
            endpoint_paths: event_stream_endpoint_paths(),
            event_operations: vec![
                "stream_create",
                "stream_status",
                "sse_subscribe",
                "websocket_posture",
                "schema_lookup",
                "asyncapi_contract",
            ],
            delivery_protocols: vec!["server_sent_events", "websocket_optional"],
            stream_id: contract.stream_id,
            stream_ref: contract.stream_ref,
            engine_mode: contract.engine_mode,
            workload_ref: contract.workload_ref,
            openapi_contract_path: OPENAPI_CONTRACT_PATH,
            asyncapi_version: ASYNCAPI_VERSION,
            asyncapi_contract_path: ASYNCAPI_EVENT_CONTRACT_PATH,
            cloudevents_spec_version: "1.0",
            cloudevents_required_fields: vec![
                "specversion",
                "id",
                "type",
                "source",
                "subject",
                "time",
                "datacontenttype",
                "dataschema",
                "data",
            ],
            event_contracts: event_stream_event_contracts(),
            sse_first: true,
            sse_media_type: "text/event-stream",
            websocket_supported: true,
            websocket_required: false,
            bidirectional_interaction_required: false,
            event_count: event_stream_detailed_event_count(&contract),
            progress_event_count: contract.progress_event_count,
            state_event_count: contract.state_event_count,
            checkpoint_event_count: contract.checkpoint_event_count,
            watermark_event_count: contract.watermark_event_count,
            certificate_event_count: contract.certificate_event_count,
            lineage_event_count: contract.lineage_event_count,
            benchmark_event_count: contract.benchmark_event_count,
            hot_cold_contribution_event_count: contract.hot_cold_contribution_event_count,
            live_fixture_certified: contract.live_fixture_certified,
            hybrid_fixture_certified: contract.hybrid_fixture_certified,
            workload_certified: contract.workload_certified,
            cg22_workload_evidence_present: contract.cg22_workload_evidence_present,
            cg8_runtime_evidence_present: contract.cg8_runtime_evidence_present,
            cg4_checkpoint_evidence_present: contract.cg4_checkpoint_evidence_present,
            cg16_execution_certificate_present: contract.cg16_execution_certificate_present,
            production_claim_allowed: contract.production_claim_allowed,
            broker_requested: contract.broker_requested,
            broker_required: contract.broker_required,
            object_store_required: contract.object_store_required,
            freshness_certificate_ref: contract.freshness_certificate_ref,
            state_certificate_ref: contract.state_certificate_ref,
            continuous_view_certificate_ref: contract.continuous_view_certificate_ref,
            delta_overlay_certificate_ref: contract.delta_overlay_certificate_ref,
            micro_segment_flush_evidence_ref: contract.micro_segment_flush_evidence_ref,
            hot_cold_contribution_report_ref: contract.hot_cold_contribution_report_ref,
            execution_certificate_ref: contract.execution_certificate_ref,
            native_io_certificate_ref: contract.native_io_certificate_ref,
            lineage_artifact_ref: contract.lineage_artifact_ref,
            benchmark_event_ref: contract.benchmark_event_ref,
            no_fallback_evidence_artifact_ref: "artifacts/cg23/event-stream/no-fallback.json",
            server_started: false,
            network_listener_opened: false,
            broker_io: false,
            object_store_io: false,
            dataset_probe: false,
            catalog_probe: false,
            credential_resolution: false,
            data_read: false,
            data_materialized: false,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            execution_delegated: false,
            diagnostics: contract.diagnostics,
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        match self.event_stream_status {
            RestApiEventStreamStatus::CertifiedFixture => CommandStatus::Success,
            RestApiEventStreamStatus::BlockedMissingEvidence => CommandStatus::Warning,
            RestApiEventStreamStatus::UnsupportedExternalBroker => CommandStatus::Unsupported,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.effect_policy_violated()
            || self
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }

    #[must_use]
    pub const fn effect_policy_violated(&self) -> bool {
        self.server_started
            || self.network_listener_opened
            || self.broker_io
            || self.object_store_io
            || self.dataset_probe
            || self.catalog_probe
            || self.credential_resolution
            || self.data_read
            || self.data_materialized
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.execution_delegated
    }

    #[must_use]
    pub fn event_type_summary(&self) -> String {
        self.event_contracts
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn event_contract_summary(&self) -> String {
        self.event_contracts
            .iter()
            .map(|event| format!("{}:{}", event.event_type, event.cloudevents_type))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn certificate_ref_summary(&self) -> String {
        [
            self.freshness_certificate_ref,
            self.state_certificate_ref,
            self.continuous_view_certificate_ref,
            self.delta_overlay_certificate_ref,
            self.micro_segment_flush_evidence_ref,
            self.execution_certificate_ref,
            self.native_io_certificate_ref,
        ]
        .into_iter()
        .filter(|value| *value != "none")
        .collect::<Vec<_>>()
        .join(",")
    }

    #[must_use]
    pub fn cloudevents_required_field_summary(&self) -> String {
        self.cloudevents_required_fields.join(",")
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api event stream\nschema_version: {}\nreport: {}\nscenario: {}\nstatus: {}\nstream: {}\nengine mode: {}\nprotocols: {}\nevents: {}\nasyncapi: {}\nserver started: false\nbroker io: false\nobject store io: false\nexternal engine: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scenario.as_str(),
            self.event_stream_status.as_str(),
            self.stream_ref,
            self.engine_mode,
            self.delivery_protocols.join(","),
            self.event_type_summary(),
            self.asyncapi_contract_path,
        )
    }
}

impl RestApiSecurityGovernanceReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn for_scenario(scenario: RestApiSecurityGovernanceScenario) -> Self {
        let contract = security_governance_scenario_contract(scenario);

        Self {
            schema_version: REST_API_SECURITY_GOVERNANCE_SCHEMA_VERSION,
            report_id: "cg23.rest_api_security_governance",
            api_version: API_VERSION,
            scenario,
            governance_status: contract.governance_status,
            endpoint_paths: security_governance_endpoint_paths(),
            governance_operations: vec![
                "auth_posture",
                "scope_matrix",
                "audit_policy",
                "redaction_policy",
                "mcp_resource_discovery",
                "mcp_tool_discovery",
                "observability_evidence_model",
            ],
            auth_postures: security_governance_auth_postures(),
            scopes: security_governance_scopes(),
            audit_policies: security_governance_audit_policies(),
            mcp_resources: security_governance_mcp_resources(),
            mcp_tools: security_governance_mcp_tools(),
            evidence_model: security_governance_evidence_model(),
            openapi_contract_path: OPENAPI_CONTRACT_PATH,
            problem_details_media_type: PROBLEM_DETAILS_MEDIA_TYPE,
            problem_details: contract.problem_details,
            local_only_default: true,
            credential_references_only: true,
            credentials_resolved: false,
            token_secret_ref: "secret-ref://shardloom/rest/token",
            mtls_certificate_ref: "cert-ref://shardloom/rest/mtls-client",
            oidc_issuer_ref: "issuer-ref://shardloom/rest/oidc",
            service_account_ref: "service-account-ref://shardloom/rest/local-agent",
            raw_secret_values_present: false,
            secrets_redacted: true,
            redaction_policy: "strict_reference_only",
            destructive_operation_requested: contract.destructive_operation_requested,
            destructive_policy_required: true,
            destructive_policy_present: contract.destructive_policy_present,
            destructive_operations_allowed: false,
            audit_required: true,
            audit_evidence_ref: "artifacts/cg23/security-governance/audit-policy.json",
            mcp_dry_run_default: true,
            mcp_effectful_tools_allowed: false,
            mcp_discovery_side_effect_free: contract.agent_discovery_requested
                || matches!(
                    scenario,
                    RestApiSecurityGovernanceScenario::SafeLocalDefault
                        | RestApiSecurityGovernanceScenario::DestructivePolicyRequired
                ),
            opentelemetry_exporter_enabled: false,
            runtime_collection_enabled: false,
            openlineage_facets_mapped: true,
            problem_details_mapped: true,
            cloudevents_mapped: true,
            certificate_refs_mapped: true,
            server_started: false,
            network_listener_opened: false,
            dataset_probe: false,
            object_store_io: false,
            catalog_probe: false,
            credential_resolution: false,
            secret_resolution: false,
            raw_secret_emitted: false,
            audit_write_io: false,
            mcp_tool_execution: false,
            data_read: false,
            data_materialized: false,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            execution_delegated: false,
            diagnostics: contract.diagnostics,
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.effect_policy_violated() {
            CommandStatus::Error
        } else {
            match self.governance_status {
                RestApiSecurityGovernanceStatus::AvailableContract
                | RestApiSecurityGovernanceStatus::AgentDryRunOnly => CommandStatus::Success,
                RestApiSecurityGovernanceStatus::BlockedPolicyRequired => {
                    CommandStatus::Unsupported
                }
            }
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.effect_policy_violated()
            || self.raw_secret_values_present
            || !self.secrets_redacted
            || self
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }

    #[must_use]
    pub const fn effect_policy_violated(&self) -> bool {
        self.server_started
            || self.network_listener_opened
            || self.dataset_probe
            || self.object_store_io
            || self.catalog_probe
            || self.credential_resolution
            || self.secret_resolution
            || self.raw_secret_emitted
            || self.audit_write_io
            || self.mcp_tool_execution
            || self.data_read
            || self.data_materialized
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.execution_delegated
    }

    #[must_use]
    pub fn auth_posture_summary(&self) -> String {
        self.auth_postures
            .iter()
            .map(|auth| format!("{}:{}", auth.auth_kind, auth.status))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn scope_summary(&self) -> String {
        self.scopes
            .iter()
            .map(|scope| format!("{}:{}", scope.scope, scope.default_access))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn audit_policy_summary(&self) -> String {
        self.audit_policies
            .iter()
            .map(|policy| format!("{}:{}", policy.event_type, policy.action))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn mcp_resource_summary(&self) -> String {
        self.mcp_resources
            .iter()
            .map(|contract| contract.name)
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn mcp_tool_summary(&self) -> String {
        self.mcp_tools
            .iter()
            .map(|contract| format!("{}:{}", contract.name, contract.default_operation))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn evidence_signal_summary(&self) -> String {
        self.evidence_model
            .iter()
            .map(|signal| signal.signal)
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn problem_details_emitted(&self) -> bool {
        self.problem_details.is_some()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api security governance\nschema_version: {}\nreport: {}\nscenario: {}\nstatus: {}\nauth: {}\nscopes: {}\nmcp tools: {}\nevidence model: {}\nsecrets redacted: {}\ndestructive operations allowed: false\nserver started: false\nnetwork listener: false\ncredential resolution: false\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scenario.as_str(),
            self.governance_status.as_str(),
            self.auth_posture_summary(),
            self.scope_summary(),
            self.mcp_tool_summary(),
            self.evidence_signal_summary(),
            self.secrets_redacted,
        )
    }
}

impl RestApiDataPlaneReport {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn for_scenario(scenario: RestApiDataPlaneScenario) -> Self {
        let contract = data_plane_scenario_contract(scenario);

        Self {
            schema_version: REST_API_DATA_PLANE_SCHEMA_VERSION,
            report_id: "cg23.rest_api_data_plane",
            api_version: API_VERSION,
            scenario,
            data_plane_status: contract.data_plane_status,
            endpoint_paths: data_plane_endpoint_paths(),
            data_plane_operations: vec![
                "result_transfer_policy",
                "large_payload_policy",
                "flight_ticket_posture",
                "adbc_endpoint_posture",
                "standards_boundary_matrix",
            ],
            transfer_contracts: data_plane_transfer_contracts(),
            standards: standards_boundary_contracts(),
            openapi_contract_path: OPENAPI_CONTRACT_PATH,
            rest_control_plane_required: true,
            rest_control_plane_sufficient_for_local_use: true,
            flight_adbc_required_for_basic_local_use: false,
            flight_ticket_requested: contract.flight_ticket_requested,
            flight_ticket_supported: false,
            adbc_endpoint_requested: contract.adbc_endpoint_requested,
            adbc_endpoint_supported: false,
            optional_transport_required: contract.optional_transport_required,
            large_payload_threshold_bytes: 1_048_576,
            preferred_large_payload_modes: vec![
                "vortex_artifact",
                "object_reference",
                "paged_json",
            ],
            inline_json_max_bytes: 1_048_576,
            paged_json_available: true,
            jsonl_ndjson_available: true,
            vortex_artifact_available: true,
            object_reference_available: true,
            arrow_ipc_decoded_boundary_available: true,
            arrow_ipc_certified_native: false,
            decoded_columnar_boundary_declared: true,
            materialization_declared: true,
            fidelity_declared: true,
            result_policy_declared: true,
            no_fallback_evidence_artifact_ref: "artifacts/cg23/data-plane/no-fallback.json",
            security_governance_policy_ref: "cg23.rest_api_security_governance",
            standards_matrix_requested: contract.standards_matrix_requested,
            standards_matrix_count: standards_boundary_contracts().len(),
            server_started: false,
            network_listener_opened: false,
            flight_server_started: false,
            adbc_endpoint_opened: false,
            broker_io: false,
            object_store_io: false,
            catalog_probe: false,
            dataset_probe: false,
            credential_resolution: false,
            data_read: false,
            data_materialized: false,
            query_execution: false,
            runtime_execution: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            execution_delegated: false,
            diagnostics: contract.diagnostics,
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.effect_policy_violated() {
            CommandStatus::Error
        } else {
            match self.data_plane_status {
                RestApiDataPlaneStatus::ContractAvailable
                | RestApiDataPlaneStatus::StandardsMatrixAvailable => CommandStatus::Success,
                RestApiDataPlaneStatus::OptionalTransportPlanned => CommandStatus::Warning,
            }
        }
    }

    #[must_use]
    pub const fn effect_policy_violated(&self) -> bool {
        self.server_started
            || self.network_listener_opened
            || self.flight_server_started
            || self.adbc_endpoint_opened
            || self.broker_io
            || self.object_store_io
            || self.catalog_probe
            || self.dataset_probe
            || self.credential_resolution
            || self.data_read
            || self.data_materialized
            || self.query_execution
            || self.runtime_execution
            || self.write_io
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.execution_delegated
    }

    #[must_use]
    pub fn transfer_mode_summary(&self) -> String {
        self.transfer_contracts
            .iter()
            .map(|transfer| format!("{}:{}", transfer.mode, transfer.materialization))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn standards_summary(&self) -> String {
        self.standards
            .iter()
            .map(|standard| format!("{}:{}", standard.standard, standard.posture))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn standards_name_summary(&self) -> String {
        self.standards
            .iter()
            .map(|standard| standard.standard)
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn optional_transport_summary(&self) -> String {
        format!(
            "flight_ticket:supported={},required={};adbc_endpoint:supported={},required={}",
            self.flight_ticket_supported,
            self.flight_adbc_required_for_basic_local_use,
            self.adbc_endpoint_supported,
            self.flight_adbc_required_for_basic_local_use
        )
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "rest api data plane\nschema_version: {}\nreport: {}\nscenario: {}\nstatus: {}\nrest control plane sufficient: {}\noptional transports: {}\ntransfers: {}\nstandards: {}\nserver started: false\nflight server: false\nadbc endpoint: false\nbroker io: false\nobject store io: false\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.scenario.as_str(),
            self.data_plane_status.as_str(),
            self.rest_control_plane_sufficient_for_local_use,
            self.optional_transport_summary(),
            self.transfer_mode_summary(),
            self.standards_name_summary(),
        )
    }
}

fn rest_api_maturity_stages() -> Vec<RestApiMaturityStage> {
    vec![
        RestApiMaturityStage {
            stage_id: "API-A1",
            label: "openapi_contract_no_server",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: false,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A2",
            label: "local_loopback_discovery_contract",
            status: RestApiMaturityStatus::ContractDeclaredNoListener,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A3",
            label: "plan_explain_validate_certify_preview",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A4",
            label: "async_query_lifecycle_certified_local_batch",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: true,
        },
        RestApiMaturityStage {
            stage_id: "API-A5",
            label: "result_delivery_and_spooling",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A6",
            label: "source_sink_adapter_api_with_native_io",
            status: RestApiMaturityStatus::BlockedUntilEvidence,
            server_required: true,
            execution_capable: true,
        },
        RestApiMaturityStage {
            stage_id: "API-A7",
            label: "live_hybrid_event_api",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A8",
            label: "security_governance_quotas_audit",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A9",
            label: "columnar_data_plane_and_standards_boundary",
            status: RestApiMaturityStatus::AvailableContract,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A10",
            label: "production_certified_workload_api",
            status: RestApiMaturityStatus::BlockedUntilEvidence,
            server_required: true,
            execution_capable: true,
        },
    ]
}

fn local_lifecycle_endpoint_paths() -> Vec<&'static str> {
    vec![
        "/v1/queries",
        "/v1/queries/{query_id}",
        "/v1/queries/{query_id}/cancel",
        "/v1/queries/{query_id}/retry",
        "/v1/queries/{query_id}/profile",
        "/v1/queries/{query_id}/lineage",
        "/v1/results/{result_id}",
        "/v1/results/{result_id}/pages",
        "/v1/results/{result_id}/jsonl",
        "/v1/results/{result_id}/artifact",
        "/v1/certificates/{certificate_id}",
        "/v1/profiles/{profile_id}",
        "/v1/artifacts/{artifact_id}",
    ]
}

const fn local_lifecycle_plan_handle(scenario: RestApiLocalLifecycleScenario) -> &'static str {
    match scenario {
        RestApiLocalLifecycleScenario::CertifiedLocalBatch => "plan://cg23/certified-local-batch",
        RestApiLocalLifecycleScenario::CancelRequested => "plan://cg23/cancel-requested",
        RestApiLocalLifecycleScenario::RetryRequested => "plan://cg23/retry-requested",
        RestApiLocalLifecycleScenario::BlockedUncertified => "plan://cg23/blocked-uncertified",
    }
}

fn local_lifecycle_result_policies() -> Vec<RestApiResultPolicyContract> {
    vec![
        RestApiResultPolicyContract {
            mode: "inline_json",
            materialization: "decoded_rows",
            certified_native: true,
            preferred_for_high_fidelity: false,
        },
        RestApiResultPolicyContract {
            mode: "paged_json",
            materialization: "decoded_rows",
            certified_native: true,
            preferred_for_high_fidelity: false,
        },
        RestApiResultPolicyContract {
            mode: "jsonl_ndjson",
            materialization: "decoded_rows",
            certified_native: true,
            preferred_for_high_fidelity: false,
        },
        RestApiResultPolicyContract {
            mode: "arrow_ipc_decoded_boundary",
            materialization: "decoded_columnar_boundary",
            certified_native: false,
            preferred_for_high_fidelity: false,
        },
        RestApiResultPolicyContract {
            mode: "vortex_artifact",
            materialization: "native_vortex_artifact",
            certified_native: true,
            preferred_for_high_fidelity: true,
        },
        RestApiResultPolicyContract {
            mode: "object_reference",
            materialization: "native_object_reference_future",
            certified_native: false,
            preferred_for_high_fidelity: true,
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn lifecycle_scenario_contract(
    scenario: RestApiLocalLifecycleScenario,
) -> LocalLifecycleScenarioContract {
    match scenario {
        RestApiLocalLifecycleScenario::CertifiedLocalBatch => LocalLifecycleScenarioContract {
            lifecycle_status: RestApiLocalLifecycleStatus::Succeeded,
            query_id: "query://cg23/certified-local-batch/0001",
            result_id: "result://cg23/certified-local-batch/0001",
            result_ref: "result://cg23/certified-local-batch/0001",
            result_artifact_ref: "artifacts/cg23/certified-local-batch/result.vortex",
            lifecycle_events: lifecycle_events(&[
                (
                    "execute_requested",
                    "accepted",
                    "certified local batch execute request accepted",
                ),
                (
                    "status_running",
                    "running",
                    "query lifecycle reached running",
                ),
                (
                    "result_ready",
                    "succeeded",
                    "inline and artifact result handles are ready",
                ),
                (
                    "certificates_ready",
                    "succeeded",
                    "execution and Native I/O certificate refs are ready",
                ),
                ("profile_ready", "succeeded", "profile report ref is ready"),
                (
                    "lineage_ready",
                    "succeeded",
                    "lineage artifact ref is ready",
                ),
                (
                    "retention_started",
                    "retained",
                    "local ephemeral retention window started",
                ),
            ]),
            non_certified_path_blocked: false,
            cancellation_requested: false,
            cancellation_status: "not_requested",
            cancel_diagnostic_code: "none",
            retry_requested: false,
            retry_status: "not_requested",
            retry_diagnostic_code: "none",
            query_execution: true,
            runtime_execution: true,
            local_execution_performed: true,
            diagnostics: Vec::new(),
        },
        RestApiLocalLifecycleScenario::CancelRequested => LocalLifecycleScenarioContract {
            lifecycle_status: RestApiLocalLifecycleStatus::Canceled,
            query_id: "query://cg23/cancel-requested/0001",
            result_id: "none",
            result_ref: "none",
            result_artifact_ref: "none",
            lifecycle_events: lifecycle_events(&[
                (
                    "execute_requested",
                    "accepted",
                    "certified local batch execute request accepted",
                ),
                (
                    "cancel_requested",
                    "accepted",
                    "cancel request accepted before result delivery",
                ),
                (
                    "status_canceled",
                    "canceled",
                    "query lifecycle reached canceled",
                ),
            ]),
            non_certified_path_blocked: false,
            cancellation_requested: true,
            cancellation_status: "canceled",
            cancel_diagnostic_code: "SL_NO_FALLBACK_EXECUTION",
            retry_requested: false,
            retry_status: "not_requested",
            retry_diagnostic_code: "none",
            query_execution: false,
            runtime_execution: false,
            local_execution_performed: false,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::NoFallbackExecution,
                DiagnosticSeverity::Info,
                DiagnosticCategory::Planning,
                "Cancellation preview completed without fallback or external engine delegation.",
                Some("rest_api_cancel".to_string()),
                Some("Cancellation is modeled in the local lifecycle contract.".to_string()),
                Some("Inspect lifecycle events for the terminal canceled state.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        },
        RestApiLocalLifecycleScenario::RetryRequested => LocalLifecycleScenarioContract {
            lifecycle_status: RestApiLocalLifecycleStatus::RetryScheduled,
            query_id: "query://cg23/retry-requested/0001",
            result_id: "none",
            result_ref: "none",
            result_artifact_ref: "none",
            lifecycle_events: lifecycle_events(&[
                (
                    "execute_requested",
                    "accepted",
                    "certified local batch execute request accepted",
                ),
                (
                    "retry_requested",
                    "accepted",
                    "retry request accepted after transient readiness signal",
                ),
                (
                    "retry_scheduled",
                    "scheduled",
                    "retry is scheduled without immediate execution",
                ),
            ]),
            non_certified_path_blocked: false,
            cancellation_requested: false,
            cancellation_status: "not_requested",
            cancel_diagnostic_code: "none",
            retry_requested: true,
            retry_status: "scheduled",
            retry_diagnostic_code: "SL_RESOURCE_BUDGET_EXCEEDED",
            query_execution: false,
            runtime_execution: false,
            local_execution_performed: false,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::ResourceBudgetExceeded,
                DiagnosticSeverity::Warning,
                DiagnosticCategory::ResourceBudget,
                "Retry preview scheduled after a transient resource-budget signal.",
                Some("rest_api_retry".to_string()),
                Some(
                    "Retry policy is explicit and does not invoke fallback execution.".to_string(),
                ),
                Some("Inspect retry_status and lifecycle events before resubmitting.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        },
        RestApiLocalLifecycleScenario::BlockedUncertified => LocalLifecycleScenarioContract {
            lifecycle_status: RestApiLocalLifecycleStatus::Blocked,
            query_id: "query://cg23/blocked-uncertified/0001",
            result_id: "none",
            result_ref: "none",
            result_artifact_ref: "none",
            lifecycle_events: lifecycle_events(&[
                (
                    "execute_requested",
                    "blocked",
                    "execute request rejected before runtime",
                ),
                (
                    "status_blocked",
                    "blocked",
                    "query lifecycle remains blocked",
                ),
                (
                    "certificates_missing",
                    "blocked",
                    "required Native I/O and execution certificates are missing",
                ),
            ]),
            non_certified_path_blocked: true,
            cancellation_requested: false,
            cancellation_status: "not_available",
            cancel_diagnostic_code: "none",
            retry_requested: false,
            retry_status: "not_available",
            retry_diagnostic_code: "none",
            query_execution: false,
            runtime_execution: false,
            local_execution_performed: false,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "rest_api_uncertified_lifecycle",
                "Non-certified REST lifecycle requests are blocked before execution.",
                Some(
                    "Use certified local batch handles or inspect plan-preview blockers."
                        .to_string(),
                ),
            )],
        },
    }
}

fn lifecycle_events(
    specs: &[(&'static str, &'static str, &'static str)],
) -> Vec<RestApiLifecycleEvent> {
    specs
        .iter()
        .map(|(event_id, status, summary)| RestApiLifecycleEvent {
            event_id,
            status,
            summary,
        })
        .collect()
}

fn event_stream_endpoint_paths() -> Vec<&'static str> {
    vec![
        "/v1/events/streams",
        "/v1/events/streams/{stream_id}",
        "/v1/events/streams/{stream_id}/sse",
        "/v1/events/streams/{stream_id}/websocket",
        "/v1/events/schemas",
        "/v1/events/asyncapi",
    ]
}

fn event_stream_event_contracts() -> Vec<RestApiEventStreamEventContract> {
    vec![
        RestApiEventStreamEventContract {
            event_type: "progress",
            category: "progress",
            cloudevents_type: "dev.shardloom.query.progress.v1",
            subject: "query.progress",
            data_schema_ref: "#/components/schemas/EventProgressData",
            evidence_ref: "artifacts/cg23/event-stream/progress.json",
            certificate_ref: "none",
        },
        RestApiEventStreamEventContract {
            event_type: "state",
            category: "state",
            cloudevents_type: "dev.shardloom.query.state.v1",
            subject: "query.state",
            data_schema_ref: "#/components/schemas/EventStateData",
            evidence_ref: "artifacts/cg23/event-stream/state.json",
            certificate_ref: "certificates/cg22/live/fixture/state.json",
        },
        RestApiEventStreamEventContract {
            event_type: "checkpoint",
            category: "checkpoint",
            cloudevents_type: "dev.shardloom.checkpoint.v1",
            subject: "checkpoint",
            data_schema_ref: "#/components/schemas/EventCheckpointData",
            evidence_ref: "artifacts/cg23/event-stream/checkpoint.json",
            certificate_ref: "certificates/cg22/live/fixture/state.json",
        },
        RestApiEventStreamEventContract {
            event_type: "watermark",
            category: "watermark",
            cloudevents_type: "dev.shardloom.watermark.v1",
            subject: "watermark",
            data_schema_ref: "#/components/schemas/EventWatermarkData",
            evidence_ref: "artifacts/cg23/event-stream/watermark.json",
            certificate_ref: "certificates/cg22/live/fixture/freshness.json",
        },
        RestApiEventStreamEventContract {
            event_type: "certificate",
            category: "certificate",
            cloudevents_type: "dev.shardloom.certificate.ready.v1",
            subject: "certificate",
            data_schema_ref: "#/components/schemas/EventCertificateData",
            evidence_ref: "artifacts/cg23/event-stream/certificates.json",
            certificate_ref: "certificates/cg22/live/fixture/group-count/execution.json",
        },
        RestApiEventStreamEventContract {
            event_type: "lineage",
            category: "lineage",
            cloudevents_type: "dev.shardloom.lineage.ready.v1",
            subject: "lineage",
            data_schema_ref: "#/components/schemas/EventLineageData",
            evidence_ref: "artifacts/cg23/event-stream/lineage.json",
            certificate_ref: "none",
        },
        RestApiEventStreamEventContract {
            event_type: "benchmark",
            category: "benchmark",
            cloudevents_type: "dev.shardloom.benchmark.evidence.v1",
            subject: "benchmark",
            data_schema_ref: "#/components/schemas/EventBenchmarkData",
            evidence_ref: "artifacts/cg23/event-stream/benchmark.json",
            certificate_ref: "none",
        },
        RestApiEventStreamEventContract {
            event_type: "hybrid_hot_cold_contribution",
            category: "hybrid",
            cloudevents_type: "dev.shardloom.hybrid.contribution.v1",
            subject: "hybrid.hot_cold_contribution",
            data_schema_ref: "#/components/schemas/EventHybridContributionData",
            evidence_ref: "artifacts/cg22/hybrid/fixture/hot-cold-contribution.json",
            certificate_ref: "certificates/cg22/hybrid/fixture/delta-overlay.json",
        },
    ]
}

fn event_stream_detailed_event_count(contract: &EventStreamScenarioContract) -> u32 {
    contract.progress_event_count
        + contract.state_event_count
        + contract.checkpoint_event_count
        + contract.watermark_event_count
        + contract.certificate_event_count
        + contract.lineage_event_count
        + contract.benchmark_event_count
        + contract.hot_cold_contribution_event_count
}

#[allow(clippy::too_many_lines)]
fn event_stream_scenario_contract(
    scenario: RestApiEventStreamScenario,
) -> EventStreamScenarioContract {
    match scenario {
        RestApiEventStreamScenario::CertifiedLiveFixture => EventStreamScenarioContract {
            event_stream_status: RestApiEventStreamStatus::CertifiedFixture,
            stream_id: "event-stream://cg23/live-fixture/group-count",
            stream_ref: "event-stream://cg23/live-fixture/group-count",
            engine_mode: "live",
            workload_ref: "fixture://cg22/live/group-count",
            progress_event_count: 1,
            state_event_count: 1,
            checkpoint_event_count: 1,
            watermark_event_count: 1,
            certificate_event_count: 1,
            lineage_event_count: 1,
            benchmark_event_count: 1,
            hot_cold_contribution_event_count: 0,
            live_fixture_certified: true,
            hybrid_fixture_certified: false,
            workload_certified: true,
            cg22_workload_evidence_present: true,
            cg8_runtime_evidence_present: true,
            cg4_checkpoint_evidence_present: true,
            cg16_execution_certificate_present: true,
            production_claim_allowed: false,
            broker_requested: false,
            broker_required: false,
            object_store_required: false,
            freshness_certificate_ref: "certificates/cg22/live/fixture/freshness.json",
            state_certificate_ref: "certificates/cg22/live/fixture/state.json",
            continuous_view_certificate_ref: "certificates/cg22/live/fixture/continuous-view.json",
            delta_overlay_certificate_ref: "none",
            micro_segment_flush_evidence_ref: "none",
            hot_cold_contribution_report_ref: "none",
            execution_certificate_ref: "certificates/cg22/live/fixture/group-count/execution.json",
            native_io_certificate_ref: "certificates/cg22/live/fixture/group-count/native-io.json",
            lineage_artifact_ref: "artifacts/cg23/event-stream/live-fixture/lineage.json",
            benchmark_event_ref: "artifacts/cg23/event-stream/live-fixture/benchmark.json",
            diagnostics: Vec::new(),
        },
        RestApiEventStreamScenario::CertifiedHybridFixture => EventStreamScenarioContract {
            event_stream_status: RestApiEventStreamStatus::CertifiedFixture,
            stream_id: "event-stream://cg23/hybrid-fixture/group-count",
            stream_ref: "event-stream://cg23/hybrid-fixture/group-count",
            engine_mode: "hybrid",
            workload_ref: "fixture://cg22/hybrid/group-count",
            progress_event_count: 1,
            state_event_count: 0,
            checkpoint_event_count: 1,
            watermark_event_count: 1,
            certificate_event_count: 1,
            lineage_event_count: 1,
            benchmark_event_count: 1,
            hot_cold_contribution_event_count: 1,
            live_fixture_certified: false,
            hybrid_fixture_certified: true,
            workload_certified: true,
            cg22_workload_evidence_present: true,
            cg8_runtime_evidence_present: true,
            cg4_checkpoint_evidence_present: true,
            cg16_execution_certificate_present: true,
            production_claim_allowed: false,
            broker_requested: false,
            broker_required: false,
            object_store_required: false,
            freshness_certificate_ref: "certificates/cg22/hybrid/fixture/freshness.json",
            state_certificate_ref: "none",
            continuous_view_certificate_ref: "none",
            delta_overlay_certificate_ref: "certificates/cg22/hybrid/fixture/delta-overlay.json",
            micro_segment_flush_evidence_ref: "artifacts/cg22/hybrid/fixture/micro-segment-flush.json",
            hot_cold_contribution_report_ref: "artifacts/cg22/hybrid/fixture/hot-cold-contribution.json",
            execution_certificate_ref: "certificates/cg22/hybrid/fixture/group-count/execution.json",
            native_io_certificate_ref: "certificates/cg22/hybrid/fixture/group-count/native-io.json",
            lineage_artifact_ref: "artifacts/cg23/event-stream/hybrid-fixture/lineage.json",
            benchmark_event_ref: "artifacts/cg23/event-stream/hybrid-fixture/benchmark.json",
            diagnostics: Vec::new(),
        },
        RestApiEventStreamScenario::BlockedProductionWorkload => EventStreamScenarioContract {
            event_stream_status: RestApiEventStreamStatus::BlockedMissingEvidence,
            stream_id: "event-stream://cg23/production-live/blocked",
            stream_ref: "event-stream://cg23/production-live/blocked",
            engine_mode: "live",
            workload_ref: "workload://cg23/production-live/unscoped",
            progress_event_count: 0,
            state_event_count: 0,
            checkpoint_event_count: 0,
            watermark_event_count: 0,
            certificate_event_count: 0,
            lineage_event_count: 0,
            benchmark_event_count: 0,
            hot_cold_contribution_event_count: 0,
            live_fixture_certified: false,
            hybrid_fixture_certified: false,
            workload_certified: false,
            cg22_workload_evidence_present: false,
            cg8_runtime_evidence_present: false,
            cg4_checkpoint_evidence_present: false,
            cg16_execution_certificate_present: false,
            production_claim_allowed: false,
            broker_requested: false,
            broker_required: false,
            object_store_required: false,
            freshness_certificate_ref: "none",
            state_certificate_ref: "none",
            continuous_view_certificate_ref: "none",
            delta_overlay_certificate_ref: "none",
            micro_segment_flush_evidence_ref: "none",
            hot_cold_contribution_report_ref: "none",
            execution_certificate_ref: "none",
            native_io_certificate_ref: "none",
            lineage_artifact_ref: "none",
            benchmark_event_ref: "none",
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::NotImplemented,
                DiagnosticSeverity::Warning,
                DiagnosticCategory::Planning,
                "Production live/hybrid event API certification is blocked until workload-scoped CG-22, CG-8, CG-4, and CG-16 evidence exists.",
                Some("rest_api_event_stream".to_string()),
                Some("Fixture event stream contracts are available, but this workload has no certified live/hybrid dossier.".to_string()),
                Some("Run a workload-scoped certification dossier before enabling production event streaming.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        },
        RestApiEventStreamScenario::BrokerRequested => EventStreamScenarioContract {
            event_stream_status: RestApiEventStreamStatus::UnsupportedExternalBroker,
            stream_id: "event-stream://cg23/broker-requested/blocked",
            stream_ref: "event-stream://cg23/broker-requested/blocked",
            engine_mode: "hybrid",
            workload_ref: "workload://cg23/broker-requested/unscoped",
            progress_event_count: 0,
            state_event_count: 0,
            checkpoint_event_count: 0,
            watermark_event_count: 0,
            certificate_event_count: 0,
            lineage_event_count: 0,
            benchmark_event_count: 0,
            hot_cold_contribution_event_count: 0,
            live_fixture_certified: false,
            hybrid_fixture_certified: false,
            workload_certified: false,
            cg22_workload_evidence_present: false,
            cg8_runtime_evidence_present: false,
            cg4_checkpoint_evidence_present: false,
            cg16_execution_certificate_present: false,
            production_claim_allowed: false,
            broker_requested: true,
            broker_required: true,
            object_store_required: false,
            freshness_certificate_ref: "none",
            state_certificate_ref: "none",
            continuous_view_certificate_ref: "none",
            delta_overlay_certificate_ref: "none",
            micro_segment_flush_evidence_ref: "none",
            hot_cold_contribution_report_ref: "none",
            execution_certificate_ref: "none",
            native_io_certificate_ref: "none",
            lineage_artifact_ref: "none",
            benchmark_event_ref: "none",
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::ExternalEffectDisabled,
                "rest_api_event_broker",
                "Broker-backed event delivery is not implemented and cannot be used as implicit execution.",
                Some("Use the SSE-first fixture event stream contract or add explicit broker certification evidence.".to_string()),
            )],
        },
    }
}

fn security_governance_endpoint_paths() -> Vec<&'static str> {
    vec![
        "/v1/governance",
        "/v1/security/auth",
        "/v1/security/scopes",
        "/v1/security/audit-policy",
        "/v1/observability/evidence-model",
        "/v1/mcp/resources",
        "/v1/mcp/tools",
    ]
}

fn security_governance_auth_postures() -> Vec<RestApiAuthPostureContract> {
    vec![
        RestApiAuthPostureContract {
            auth_kind: "local_only",
            status: "available_default",
            credential_ref: "none",
            credential_reference_only: true,
            secret_material_allowed: false,
            runtime_resolution_allowed: false,
            local_only: true,
        },
        RestApiAuthPostureContract {
            auth_kind: "token",
            status: "reference_only_contract",
            credential_ref: "secret-ref://shardloom/rest/token",
            credential_reference_only: true,
            secret_material_allowed: false,
            runtime_resolution_allowed: false,
            local_only: false,
        },
        RestApiAuthPostureContract {
            auth_kind: "mtls",
            status: "reference_only_contract",
            credential_ref: "cert-ref://shardloom/rest/mtls-client",
            credential_reference_only: true,
            secret_material_allowed: false,
            runtime_resolution_allowed: false,
            local_only: false,
        },
        RestApiAuthPostureContract {
            auth_kind: "oidc",
            status: "reference_only_contract",
            credential_ref: "issuer-ref://shardloom/rest/oidc",
            credential_reference_only: true,
            secret_material_allowed: false,
            runtime_resolution_allowed: false,
            local_only: false,
        },
        RestApiAuthPostureContract {
            auth_kind: "service_account",
            status: "reference_only_contract",
            credential_ref: "service-account-ref://shardloom/rest/local-agent",
            credential_reference_only: true,
            secret_material_allowed: false,
            runtime_resolution_allowed: false,
            local_only: false,
        },
    ]
}

fn security_governance_scopes() -> Vec<RestApiScopeContract> {
    vec![
        RestApiScopeContract {
            scope: "read",
            default_access: "allowed_local_metadata",
            policy_required: false,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "plan",
            default_access: "allowed_dry_run",
            policy_required: false,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "execute",
            default_access: "policy_required",
            policy_required: true,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "write",
            default_access: "policy_required",
            policy_required: true,
            destructive: true,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "cancel",
            default_access: "policy_required",
            policy_required: true,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "admin",
            default_access: "policy_required",
            policy_required: true,
            destructive: true,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "benchmark",
            default_access: "dry_run_only",
            policy_required: true,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "migration",
            default_access: "plan_only",
            policy_required: true,
            destructive: false,
            audit_required: true,
        },
        RestApiScopeContract {
            scope: "agent",
            default_access: "dry_run_explain_estimate_certify_only",
            policy_required: true,
            destructive: false,
            audit_required: true,
        },
    ]
}

fn security_governance_audit_policies() -> Vec<RestApiAuditPolicyContract> {
    vec![
        RestApiAuditPolicyContract {
            event_type: "auth_attempt",
            action: "record_redacted_subject_and_auth_kind",
            required: true,
            redaction_required: true,
            evidence_ref: "artifacts/cg23/security-governance/audit/auth-attempt.json",
        },
        RestApiAuditPolicyContract {
            event_type: "scope_decision",
            action: "record_allow_or_block_with_policy_ref",
            required: true,
            redaction_required: true,
            evidence_ref: "artifacts/cg23/security-governance/audit/scope-decision.json",
        },
        RestApiAuditPolicyContract {
            event_type: "destructive_operation",
            action: "block_without_explicit_policy",
            required: true,
            redaction_required: true,
            evidence_ref: "artifacts/cg23/security-governance/audit/destructive-operation.json",
        },
        RestApiAuditPolicyContract {
            event_type: "agent_tool",
            action: "record_dry_run_tool_invocation",
            required: true,
            redaction_required: true,
            evidence_ref: "artifacts/cg23/security-governance/audit/agent-tool.json",
        },
    ]
}

fn security_governance_mcp_resources() -> Vec<RestApiMcpContract> {
    vec![
        RestApiMcpContract {
            name: "shardloom://capabilities",
            contract_kind: "resource",
            default_operation: "read",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.capability_certification.v1",
        },
        RestApiMcpContract {
            name: "shardloom://api/openapi",
            contract_kind: "resource",
            default_operation: "read",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.rest_api_contract.v1",
        },
        RestApiMcpContract {
            name: "shardloom://evidence/model",
            contract_kind: "resource",
            default_operation: "read",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.rest_api_security_governance.v1",
        },
        RestApiMcpContract {
            name: "shardloom://security/policy",
            contract_kind: "resource",
            default_operation: "read",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.security_governance_evidence_gate.v1",
        },
    ]
}

fn security_governance_mcp_tools() -> Vec<RestApiMcpContract> {
    vec![
        RestApiMcpContract {
            name: "dry_run",
            contract_kind: "tool",
            default_operation: "allowed",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.output.v2",
        },
        RestApiMcpContract {
            name: "explain",
            contract_kind: "tool",
            default_operation: "allowed",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.output.v2",
        },
        RestApiMcpContract {
            name: "estimate",
            contract_kind: "tool",
            default_operation: "allowed",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.output.v2",
        },
        RestApiMcpContract {
            name: "certify_preview",
            contract_kind: "tool",
            default_operation: "allowed",
            dry_run_only: true,
            effectful: false,
            output_schema_ref: "shardloom.rest_api_plan_preview.v1",
        },
        RestApiMcpContract {
            name: "execute",
            contract_kind: "tool",
            default_operation: "blocked_policy_required",
            dry_run_only: true,
            effectful: true,
            output_schema_ref: "shardloom.rest_api_local_lifecycle.v1",
        },
        RestApiMcpContract {
            name: "write",
            contract_kind: "tool",
            default_operation: "blocked_destructive_policy_required",
            dry_run_only: true,
            effectful: true,
            output_schema_ref: "shardloom.output.v2",
        },
    ]
}

fn security_governance_evidence_model() -> Vec<RestApiEvidenceModelSignal> {
    vec![
        RestApiEvidenceModelSignal {
            signal: "opentelemetry_traces",
            standard: "opentelemetry",
            schema_ref: "schemas/observability/opentelemetry-trace-span.json",
            redaction_required: true,
            certificate_ref_required: true,
        },
        RestApiEvidenceModelSignal {
            signal: "opentelemetry_metrics",
            standard: "opentelemetry",
            schema_ref: "schemas/observability/opentelemetry-metric.json",
            redaction_required: true,
            certificate_ref_required: true,
        },
        RestApiEvidenceModelSignal {
            signal: "opentelemetry_logs",
            standard: "opentelemetry",
            schema_ref: "schemas/observability/opentelemetry-log.json",
            redaction_required: true,
            certificate_ref_required: true,
        },
        RestApiEvidenceModelSignal {
            signal: "openlineage_facets",
            standard: "openlineage",
            schema_ref: "schemas/lineage/openlineage-facet.json",
            redaction_required: true,
            certificate_ref_required: true,
        },
        RestApiEvidenceModelSignal {
            signal: "problem_details_errors",
            standard: "rfc9457_problem_details",
            schema_ref: "#/components/schemas/ProblemDetails",
            redaction_required: true,
            certificate_ref_required: false,
        },
        RestApiEvidenceModelSignal {
            signal: "cloudevents",
            standard: "cloudevents_1_0",
            schema_ref: "#/components/schemas/CloudEventEnvelope",
            redaction_required: true,
            certificate_ref_required: true,
        },
        RestApiEvidenceModelSignal {
            signal: "certificate_refs",
            standard: "shardloom_certificate_refs",
            schema_ref: "schemas/evidence/certificate-ref.json",
            redaction_required: true,
            certificate_ref_required: true,
        },
    ]
}

fn security_governance_scenario_contract(
    scenario: RestApiSecurityGovernanceScenario,
) -> SecurityGovernanceScenarioContract {
    match scenario {
        RestApiSecurityGovernanceScenario::SafeLocalDefault => SecurityGovernanceScenarioContract {
            governance_status: RestApiSecurityGovernanceStatus::AvailableContract,
            destructive_operation_requested: false,
            destructive_policy_present: false,
            agent_discovery_requested: false,
            problem_details: None,
            diagnostics: Vec::new(),
        },
        RestApiSecurityGovernanceScenario::DestructivePolicyRequired => {
            SecurityGovernanceScenarioContract {
                governance_status: RestApiSecurityGovernanceStatus::BlockedPolicyRequired,
                destructive_operation_requested: true,
                destructive_policy_present: false,
                agent_discovery_requested: false,
                problem_details: Some(RestApiProblemDetailsPreview {
                    problem_type: "https://shardloom.dev/problems/destructive-policy-required",
                    title: "Explicit destructive-operation policy is required",
                    http_status: 403,
                    detail: "The requested REST operation is destructive and no explicit policy evidence was supplied.",
                    diagnostic_code: "SL_EXTERNAL_EFFECT_DISABLED",
                    unsupported_reason: Some("destructive operations stay blocked until policy, audit, and redaction evidence are present"),
                }),
                diagnostics: vec![Diagnostic::unsupported(
                    DiagnosticCode::ExternalEffectDisabled,
                    "rest_api_destructive_operation",
                    "REST destructive operations are blocked unless explicit policy, audit, and redaction evidence are present.",
                    Some("Attach an explicit destructive-operation policy reference and re-run as a dry-run certification preview.".to_string()),
                )],
            }
        }
        RestApiSecurityGovernanceScenario::AgentMcpDiscovery => SecurityGovernanceScenarioContract {
            governance_status: RestApiSecurityGovernanceStatus::AgentDryRunOnly,
            destructive_operation_requested: false,
            destructive_policy_present: false,
            agent_discovery_requested: true,
            problem_details: None,
            diagnostics: Vec::new(),
        },
    }
}

fn data_plane_endpoint_paths() -> Vec<&'static str> {
    vec![
        "/v1/data-plane",
        "/v1/results/{result_id}/flight-ticket",
        "/v1/results/{result_id}/adbc-endpoint",
        "/v1/data-plane/standards",
    ]
}

fn data_plane_transfer_contracts() -> Vec<RestApiDataPlaneTransferContract> {
    vec![
        RestApiDataPlaneTransferContract {
            mode: "inline_json",
            transport: "rest_http",
            materialization: "decoded_rows",
            fidelity: "human_inspection",
            result_policy: "small_result_only",
            preferred_for_large_payloads: false,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: false,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "paged_json",
            transport: "rest_http",
            materialization: "decoded_rows",
            fidelity: "bounded_page_inspection",
            result_policy: "large_payload_safe_default",
            preferred_for_large_payloads: true,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: false,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "jsonl_ndjson",
            transport: "rest_http",
            materialization: "decoded_rows",
            fidelity: "streaming_rows",
            result_policy: "large_payload_safe_default",
            preferred_for_large_payloads: true,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: false,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "vortex_artifact",
            transport: "rest_http_artifact_ref",
            materialization: "native_vortex_artifact",
            fidelity: "native_high_fidelity",
            result_policy: "preferred_large_payload",
            preferred_for_large_payloads: true,
            native_vortex_fidelity: true,
            decoded_columnar_boundary: false,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "object_reference",
            transport: "rest_http_object_ref",
            materialization: "native_object_reference_future",
            fidelity: "native_reference",
            result_policy: "preferred_large_payload_when_certified",
            preferred_for_large_payloads: true,
            native_vortex_fidelity: true,
            decoded_columnar_boundary: false,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "arrow_ipc_decoded_boundary",
            transport: "rest_http_artifact_ref",
            materialization: "decoded_columnar_boundary",
            fidelity: "decoded_columnar_interop",
            result_policy: "interop_boundary_not_native_claim",
            preferred_for_large_payloads: false,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: true,
            optional_dependency_required: false,
            enabled_by_default: true,
        },
        RestApiDataPlaneTransferContract {
            mode: "flight_ticket_future",
            transport: "flight_adbc_data_plane",
            materialization: "decoded_columnar_boundary",
            fidelity: "optional_interop_transport",
            result_policy: "optional_not_required",
            preferred_for_large_payloads: false,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: true,
            optional_dependency_required: true,
            enabled_by_default: false,
        },
        RestApiDataPlaneTransferContract {
            mode: "adbc_endpoint_future",
            transport: "flight_adbc_data_plane",
            materialization: "decoded_columnar_boundary",
            fidelity: "optional_interop_transport",
            result_policy: "optional_not_required",
            preferred_for_large_payloads: false,
            native_vortex_fidelity: false,
            decoded_columnar_boundary: true,
            optional_dependency_required: true,
            enabled_by_default: false,
        },
    ]
}

fn standards_boundary_contracts() -> Vec<RestApiStandardsBoundaryContract> {
    vec![
        standards_boundary(
            "iceberg_rest_catalog",
            "catalog_standard",
            "classified_control_plane_boundary",
            "optional_reference_only_no_catalog_probe",
            "catalog_control_plane",
        ),
        standards_boundary(
            "polaris",
            "catalog_standard",
            "classified_control_plane_boundary",
            "optional_reference_only_no_catalog_probe",
            "catalog_control_plane",
        ),
        standards_boundary(
            "gravitino",
            "catalog_standard",
            "classified_control_plane_boundary",
            "optional_reference_only_no_catalog_probe",
            "catalog_control_plane",
        ),
        standards_boundary(
            "delta_sharing",
            "sharing_standard",
            "classified_decoded_columnar_boundary",
            "optional_reference_only_no_data_fetch",
            "data_sharing_control_plane",
        ),
        standards_boundary(
            "substrait",
            "plan_interop_standard",
            "classified_plan_import_export_boundary",
            "optional_parser_dependency_deferred",
            "plan_interop",
        ),
        standards_boundary(
            "wasi_webassembly_components",
            "component_standard",
            "classified_component_sandbox_boundary",
            "optional_sandbox_dependency_deferred",
            "component_execution_boundary",
        ),
        standards_boundary(
            "nats_jetstream",
            "broker_standard",
            "classified_event_broker_boundary",
            "optional_broker_dependency_deferred",
            "event_delivery_boundary",
        ),
        standards_boundary(
            "redpanda",
            "broker_standard",
            "classified_kafka_compatible_boundary",
            "optional_broker_dependency_deferred",
            "event_delivery_boundary",
        ),
        standards_boundary(
            "kafka_compatible",
            "broker_standard",
            "classified_kafka_compatible_boundary",
            "optional_broker_dependency_deferred",
            "event_delivery_boundary",
        ),
        standards_boundary(
            "paimon",
            "table_format_standard",
            "classified_table_format_boundary",
            "optional_table_dependency_deferred",
            "table_format_boundary",
        ),
        standards_boundary(
            "fluss",
            "streaming_table_standard",
            "classified_streaming_table_boundary",
            "optional_streaming_table_dependency_deferred",
            "table_format_boundary",
        ),
    ]
}

fn standards_boundary(
    standard: &'static str,
    category: &'static str,
    posture: &'static str,
    dependency_policy: &'static str,
    control_plane_role: &'static str,
) -> RestApiStandardsBoundaryContract {
    RestApiStandardsBoundaryContract {
        standard,
        category,
        posture,
        dependency_policy,
        boundary_classification: "interop_or_reference_boundary",
        control_plane_role,
        materialization: "declared_by_transfer_policy",
        external_compute_boundary: true,
        broker_io: false,
        catalog_io: false,
        object_store_io: false,
        execution_allowed: false,
        fallback_allowed: false,
    }
}

fn data_plane_scenario_contract(scenario: RestApiDataPlaneScenario) -> DataPlaneScenarioContract {
    match scenario {
        RestApiDataPlaneScenario::ArtifactReferenceDefault => DataPlaneScenarioContract {
            data_plane_status: RestApiDataPlaneStatus::ContractAvailable,
            flight_ticket_requested: false,
            adbc_endpoint_requested: false,
            standards_matrix_requested: false,
            optional_transport_required: false,
            diagnostics: Vec::new(),
        },
        RestApiDataPlaneScenario::FlightTicketRequested => DataPlaneScenarioContract {
            data_plane_status: RestApiDataPlaneStatus::OptionalTransportPlanned,
            flight_ticket_requested: true,
            adbc_endpoint_requested: false,
            standards_matrix_requested: false,
            optional_transport_required: false,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::NotImplemented,
                DiagnosticSeverity::Warning,
                DiagnosticCategory::Planning,
                "Flight ticket delivery is an optional future data-plane posture and is not required for REST control-plane use.",
                Some("rest_api_data_plane".to_string()),
                Some("The REST result artifact/reference contract remains the certified proof surface; no Flight server is started.".to_string()),
                Some("Use vortex_artifact, object_reference, paged_json, or JSON Lines result policies until Flight is explicitly certified.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        },
        RestApiDataPlaneScenario::AdbcEndpointRequested => DataPlaneScenarioContract {
            data_plane_status: RestApiDataPlaneStatus::OptionalTransportPlanned,
            flight_ticket_requested: false,
            adbc_endpoint_requested: true,
            standards_matrix_requested: false,
            optional_transport_required: false,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::NotImplemented,
                DiagnosticSeverity::Warning,
                DiagnosticCategory::Planning,
                "ADBC endpoint delivery is an optional future data-plane posture and is not required for REST control-plane use.",
                Some("rest_api_data_plane".to_string()),
                Some("No ADBC endpoint is opened and no decoded-columnar transport is claimed as native execution.".to_string()),
                Some("Use REST artifact/reference result policies until ADBC endpoint evidence is certified.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        },
        RestApiDataPlaneScenario::StandardsMatrix => DataPlaneScenarioContract {
            data_plane_status: RestApiDataPlaneStatus::StandardsMatrixAvailable,
            flight_ticket_requested: false,
            adbc_endpoint_requested: false,
            standards_matrix_requested: true,
            optional_transport_required: false,
            diagnostics: Vec::new(),
        },
    }
}

fn plan_preview_endpoint_paths() -> Vec<&'static str> {
    vec![
        "/v1/plans",
        "/v1/plans/{plan_handle}",
        "/v1/plans/{plan_handle}/validate",
        "/v1/plans/{plan_handle}/explain",
        "/v1/plans/{plan_handle}/estimate",
        "/v1/plans/{plan_handle}/unsupported-report",
        "/v1/plans/{plan_handle}/certification-preview",
    ]
}

fn plan_preview_stage_bundle(
    stage_statuses: &[PlanPreviewStageSpec; 7],
) -> Vec<RestApiPlanPreviewStage> {
    stage_statuses
        .iter()
        .map(|(stage_id, status, diagnostic_code, summary)| {
            let stage_id = *stage_id;
            RestApiPlanPreviewStage {
                stage_id,
                label: match stage_id {
                    "parser" => "parser",
                    "binder" => "binder",
                    "native_logical" => "native logical",
                    "native_physical" => "native physical",
                    "execution_readiness" => "execution readiness",
                    "evidence_readiness" => "evidence readiness",
                    "certification" => "certification",
                    _ => stage_id,
                },
                status: *status,
                diagnostic_code: *diagnostic_code,
                summary,
            }
        })
        .collect()
}

fn plan_preview_stage_planned(stages: &[RestApiPlanPreviewStage], stage_id: &str) -> bool {
    stages
        .iter()
        .find(|stage| stage.stage_id == stage_id)
        .is_some_and(|stage| {
            matches!(
                stage.status,
                RestApiPlanStageStatus::Ready
                    | RestApiPlanStageStatus::Certified
                    | RestApiPlanStageStatus::Partial
            )
        })
}

fn discovery_endpoint_contracts() -> Vec<RestApiEndpointContract> {
    [
        ("GET", "/v1/health", "health"),
        ("GET", "/v1/version", "version"),
        ("GET", "/v1/capabilities", "capabilities"),
        ("GET", "/v1/capabilities/engines", "capabilities"),
        ("GET", "/v1/capabilities/operators", "capabilities"),
        ("GET", "/v1/capabilities/functions", "capabilities"),
        ("GET", "/v1/capabilities/sql", "capabilities"),
        ("GET", "/v1/capabilities/adapters", "capabilities"),
        ("GET", "/v1/capabilities/deployment", "capabilities"),
        ("GET", "/v1/adapters", "adapters"),
        ("GET", "/v1/sources", "sources"),
        ("GET", "/v1/sinks", "sinks"),
    ]
    .into_iter()
    .map(|(method, path, resource)| RestApiEndpointContract {
        method,
        path,
        resource,
        maturity_stage: "API-A2",
        side_effect_free: true,
        execution_policy_required: false,
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn rest_api_contract_is_contract_only_and_fallback_free() {
        let report = RestApiContractReport::contract_only();

        assert_eq!(report.openapi_version, "3.2.0");
        assert_eq!(report.openapi_contract_path, OPENAPI_CONTRACT_PATH);
        assert!(report.represented_resources.contains(&"capabilities"));
        assert!(report.represented_resources.contains(&"governance"));
        assert!(
            report
                .execution_policy_fields
                .contains(&"requested_execution_mode")
        );
        assert!(report.execution_policy_fields.contains(&"engine_mode"));
        assert_eq!(
            report.execution_mode_vocabulary,
            vec![
                "auto",
                "compatibility_import_certified",
                "prepared_vortex",
                "native_vortex",
                "direct_compatibility_transient"
            ]
        );
        assert_eq!(
            report.execution_mode_selection_schema_version,
            EXECUTION_MODE_SELECTION_REPORT_SCHEMA_VERSION
        );
        assert!(
            report
                .execution_mode_selection_fields
                .contains(&"requested_execution_mode")
        );
        assert!(
            report
                .execution_mode_selection_fields
                .contains(&"fallback_attempted")
        );
        assert_eq!(report.support_status, "report_only");
        assert_eq!(
            report.unsupported_execution_mode_diagnostic_code,
            "SL_UNSUPPORTED_EXECUTION_MODE"
        );
        assert!(report.discovery_endpoints_side_effect_free());
        assert!(!report.server_started);
        assert!(!report.network_listener_opened);
        assert!(!report.dataset_probe);
        assert!(!report.object_store_io);
        assert!(!report.catalog_probe);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.fallback_attempted);
        assert!(!report.has_errors());
    }

    #[test]
    fn rest_api_maturity_ladder_keeps_data_plane_before_production_claims() {
        let report = RestApiContractReport::contract_only();
        let data_plane = report
            .maturity_stages
            .iter()
            .find(|stage| stage.stage_id == "API-A9")
            .expect("API-A9 stage exists");
        let production = report
            .maturity_stages
            .iter()
            .find(|stage| stage.stage_id == "API-A10")
            .expect("API-A10 stage exists");

        assert_eq!(
            data_plane.label,
            "columnar_data_plane_and_standards_boundary"
        );
        assert_eq!(data_plane.status, RestApiMaturityStatus::AvailableContract);
        assert!(!data_plane.execution_capable);
        assert_eq!(production.label, "production_certified_workload_api");
        assert_eq!(
            production.status,
            RestApiMaturityStatus::BlockedUntilEvidence
        );
        assert!(production.execution_capable);
    }

    #[test]
    fn rest_api_discovery_mode_contract_does_not_start_listener() {
        let report = RestApiDiscoveryModeReport::contract_only("127.0.0.1:8787");

        assert_eq!(report.schema_version, REST_API_DISCOVERY_SCHEMA_VERSION);
        assert_eq!(report.report_id, "cg23.rest_api_discovery_mode.contract");
        assert_eq!(
            report.contract_report.schema_version,
            REST_API_CONTRACT_SCHEMA_VERSION
        );
        assert_eq!(report.mode, "discovery");
        assert_eq!(report.health_endpoint, "/v1/health");
        assert_eq!(report.capabilities_endpoint, "/v1/capabilities");
        assert!(!report.server_started);
        assert!(!report.network_listener_opened);
        assert!(!report.dataset_probe);
        assert!(!report.query_execution);
        assert!(!report.fallback_attempted);
        assert!(!report.has_errors());
    }

    #[test]
    fn rest_api_plan_preview_reports_stage_contracts_without_execution() {
        let report =
            RestApiPlanPreviewReport::for_scenario(RestApiPlanPreviewScenario::CertifiedLocalBatch);

        assert_eq!(report.schema_version, REST_API_PLAN_PREVIEW_SCHEMA_VERSION);
        assert_eq!(report.status(), CommandStatus::Success);
        assert_eq!(report.plan_handle, "plan://cg23/certified-local-batch");
        assert_eq!(
            report.stage_order(),
            vec![
                "parser",
                "binder",
                "native_logical",
                "native_physical",
                "execution_readiness",
                "evidence_readiness",
                "certification"
            ]
        );
        assert!(
            report
                .endpoint_paths
                .contains(&"/v1/plans/{plan_handle}/certification-preview")
        );
        assert!(!report.problem_details_emitted());
        assert!(!report.server_started);
        assert!(!report.network_listener_opened);
        assert!(!report.dataset_probe);
        assert!(!report.object_store_io);
        assert!(!report.catalog_probe);
        assert!(!report.query_execution);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.execution_delegated);
        assert!(!report.effect_policy_violated());
    }

    #[test]
    fn rest_api_plan_preview_problem_details_cover_invalid_and_unsupported() {
        let invalid =
            RestApiPlanPreviewReport::for_scenario(RestApiPlanPreviewScenario::InvalidInput);
        let unsupported =
            RestApiPlanPreviewReport::for_scenario(RestApiPlanPreviewScenario::UnsupportedOperator);

        assert_eq!(invalid.status(), CommandStatus::Error);
        assert_eq!(unsupported.status(), CommandStatus::Unsupported);
        assert_eq!(
            invalid
                .problem_details
                .as_ref()
                .map(|problem| problem.diagnostic_code),
            Some("SL_INVALID_INPUT")
        );
        assert_eq!(
            unsupported
                .problem_details
                .as_ref()
                .map(|problem| problem.diagnostic_code),
            Some("SL_UNSUPPORTED_SQL")
        );
        assert!(
            invalid
                .stage_status_summary()
                .contains("parser:invalid_input")
        );
        assert!(
            unsupported
                .stage_status_summary()
                .contains("native_logical:unsupported")
        );
        assert!(!invalid.native_logical_planned);
        assert!(!unsupported.native_logical_planned);
        assert!(!invalid.effect_policy_violated());
        assert!(!unsupported.effect_policy_violated());
    }

    #[test]
    fn rest_api_local_lifecycle_certified_path_links_results_and_evidence() {
        let report = RestApiLocalLifecycleReport::for_scenario(
            RestApiLocalLifecycleScenario::CertifiedLocalBatch,
        );

        assert_eq!(
            report.schema_version,
            REST_API_LOCAL_LIFECYCLE_SCHEMA_VERSION
        );
        assert_eq!(report.status(), CommandStatus::Success);
        assert_eq!(
            report.lifecycle_status,
            RestApiLocalLifecycleStatus::Succeeded
        );
        assert_eq!(
            report.result_ref,
            "result://cg23/certified-local-batch/0001"
        );
        assert!(report.inline_json_available);
        assert!(report.paged_json_available);
        assert!(report.jsonl_ndjson_available);
        assert!(report.vortex_artifact_available);
        assert!(report.arrow_ipc_available);
        assert_eq!(
            report.arrow_ipc_materialization,
            "decoded_columnar_boundary"
        );
        assert!(!report.arrow_ipc_certified_native);
        assert!(
            report
                .preferred_high_fidelity_result_modes
                .contains(&"vortex_artifact")
        );
        assert!(report.execution_certificate_ref.ends_with("execution.json"));
        assert!(report.native_io_certificate_ref.ends_with("native-io.json"));
        assert_eq!(
            report.no_fallback_evidence_artifact_ref,
            "artifacts/cg23/certified-local-batch/no-fallback.json"
        );
        assert!(
            report
                .lifecycle_event_summary()
                .contains("result_ready:succeeded")
        );
        assert!(report.query_execution);
        assert!(report.runtime_execution);
        assert!(report.local_execution_performed);
        assert!(!report.data_read);
        assert!(!report.write_io);
        assert!(!report.fallback_attempted);
        assert!(!report.effect_policy_violated());
    }

    #[test]
    fn rest_api_local_lifecycle_blocks_uncertified_and_models_cancel_retry() {
        let blocked = RestApiLocalLifecycleReport::for_scenario(
            RestApiLocalLifecycleScenario::BlockedUncertified,
        );
        let cancel = RestApiLocalLifecycleReport::for_scenario(
            RestApiLocalLifecycleScenario::CancelRequested,
        );
        let retry = RestApiLocalLifecycleReport::for_scenario(
            RestApiLocalLifecycleScenario::RetryRequested,
        );

        assert_eq!(blocked.status(), CommandStatus::Unsupported);
        assert!(blocked.non_certified_path_blocked);
        assert_eq!(blocked.plan_handle, "plan://cg23/blocked-uncertified");
        assert_eq!(blocked.no_fallback_evidence_artifact_ref, "none");
        assert!(!blocked.query_execution);
        assert!(!blocked.runtime_execution);
        assert!(!blocked.fallback_attempted);
        assert_eq!(cancel.plan_handle, "plan://cg23/cancel-requested");
        assert_eq!(cancel.no_fallback_evidence_artifact_ref, "none");
        assert_eq!(
            cancel.lifecycle_status,
            RestApiLocalLifecycleStatus::Canceled
        );
        assert!(cancel.cancellation_requested);
        assert_eq!(cancel.cancellation_status, "canceled");
        assert_eq!(cancel.cancel_diagnostic_code, "SL_NO_FALLBACK_EXECUTION");
        assert_eq!(
            retry.lifecycle_status,
            RestApiLocalLifecycleStatus::RetryScheduled
        );
        assert_eq!(retry.plan_handle, "plan://cg23/retry-requested");
        assert_eq!(retry.no_fallback_evidence_artifact_ref, "none");
        assert!(retry.retry_requested);
        assert_eq!(retry.retry_status, "scheduled");
        assert_eq!(retry.retry_diagnostic_code, "SL_RESOURCE_BUDGET_EXCEEDED");
    }

    #[test]
    fn rest_api_event_stream_certified_fixtures_expose_event_and_evidence_contracts() {
        let live = RestApiEventStreamReport::for_scenario(
            RestApiEventStreamScenario::CertifiedLiveFixture,
        );
        let hybrid = RestApiEventStreamReport::for_scenario(
            RestApiEventStreamScenario::CertifiedHybridFixture,
        );

        assert_eq!(live.schema_version, REST_API_EVENT_STREAM_SCHEMA_VERSION);
        assert_eq!(live.status(), CommandStatus::Success);
        assert_eq!(
            live.event_stream_status,
            RestApiEventStreamStatus::CertifiedFixture
        );
        assert!(live.sse_first);
        assert!(live.websocket_supported);
        assert!(!live.websocket_required);
        assert_eq!(live.asyncapi_contract_path, ASYNCAPI_EVENT_CONTRACT_PATH);
        assert_eq!(live.cloudevents_spec_version, "1.0");
        assert!(live.event_type_summary().contains("progress"));
        assert!(live.event_type_summary().contains("watermark"));
        assert!(live.live_fixture_certified);
        assert!(live.workload_certified);
        assert!(!live.production_claim_allowed);
        assert!(live.cg22_workload_evidence_present);
        assert!(live.cg8_runtime_evidence_present);
        assert!(live.cg4_checkpoint_evidence_present);
        assert!(live.cg16_execution_certificate_present);
        assert!(live.certificate_ref_summary().contains("freshness.json"));
        assert!(!live.broker_io);
        assert!(!live.object_store_io);
        assert!(!live.runtime_execution);
        assert!(!live.fallback_attempted);
        assert!(!live.effect_policy_violated());

        assert_eq!(hybrid.status(), CommandStatus::Success);
        assert!(hybrid.hybrid_fixture_certified);
        assert_eq!(hybrid.engine_mode, "hybrid");
        assert_eq!(
            hybrid.event_count,
            hybrid.progress_event_count
                + hybrid.state_event_count
                + hybrid.checkpoint_event_count
                + hybrid.watermark_event_count
                + hybrid.certificate_event_count
                + hybrid.lineage_event_count
                + hybrid.benchmark_event_count
                + hybrid.hot_cold_contribution_event_count
        );
        assert_eq!(hybrid.hot_cold_contribution_event_count, 1);
        assert!(
            hybrid
                .event_type_summary()
                .contains("hybrid_hot_cold_contribution")
        );
        assert!(
            hybrid
                .hot_cold_contribution_report_ref
                .ends_with("hot-cold-contribution.json")
        );
        assert!(
            hybrid
                .certificate_ref_summary()
                .contains("delta-overlay.json")
        );
        assert!(!hybrid.effect_policy_violated());
    }

    #[test]
    fn rest_api_event_stream_blocks_uncertified_production_and_broker_paths() {
        let blocked = RestApiEventStreamReport::for_scenario(
            RestApiEventStreamScenario::BlockedProductionWorkload,
        );
        let broker =
            RestApiEventStreamReport::for_scenario(RestApiEventStreamScenario::BrokerRequested);

        assert_eq!(blocked.status(), CommandStatus::Warning);
        assert_eq!(
            blocked.event_stream_status,
            RestApiEventStreamStatus::BlockedMissingEvidence
        );
        assert!(!blocked.workload_certified);
        assert!(!blocked.cg22_workload_evidence_present);
        assert!(!blocked.cg8_runtime_evidence_present);
        assert!(!blocked.cg4_checkpoint_evidence_present);
        assert!(!blocked.cg16_execution_certificate_present);
        assert!(!blocked.production_claim_allowed);
        assert!(!blocked.broker_io);
        assert!(!blocked.object_store_io);
        assert!(!blocked.fallback_attempted);
        assert_eq!(blocked.diagnostics[0].code, DiagnosticCode::NotImplemented);

        assert_eq!(broker.status(), CommandStatus::Unsupported);
        assert_eq!(
            broker.event_stream_status,
            RestApiEventStreamStatus::UnsupportedExternalBroker
        );
        assert!(broker.broker_requested);
        assert!(broker.broker_required);
        assert!(!broker.broker_io);
        assert!(!broker.external_engine_invoked);
        assert!(!broker.fallback_attempted);
        assert_eq!(
            broker.diagnostics[0].code,
            DiagnosticCode::ExternalEffectDisabled
        );
    }

    #[test]
    fn rest_api_security_governance_exposes_auth_scopes_mcp_and_evidence_model() {
        let report = RestApiSecurityGovernanceReport::for_scenario(
            RestApiSecurityGovernanceScenario::SafeLocalDefault,
        );

        assert_eq!(
            report.schema_version,
            REST_API_SECURITY_GOVERNANCE_SCHEMA_VERSION
        );
        assert_eq!(report.status(), CommandStatus::Success);
        assert_eq!(
            report.governance_status,
            RestApiSecurityGovernanceStatus::AvailableContract
        );
        assert!(report.endpoint_paths.contains(&"/v1/security/auth"));
        assert!(report.auth_posture_summary().contains("local_only"));
        assert!(report.auth_posture_summary().contains("token"));
        assert!(report.auth_posture_summary().contains("mtls"));
        assert!(report.auth_posture_summary().contains("oidc"));
        assert!(report.auth_posture_summary().contains("service_account"));
        assert!(
            report
                .scope_summary()
                .contains("read:allowed_local_metadata")
        );
        assert!(report.scope_summary().contains("write:policy_required"));
        assert!(
            report
                .mcp_resource_summary()
                .contains("shardloom://api/openapi")
        );
        assert!(
            report
                .mcp_tool_summary()
                .contains("certify_preview:allowed")
        );
        assert!(
            report
                .evidence_signal_summary()
                .contains("opentelemetry_traces")
        );
        assert!(
            report
                .evidence_signal_summary()
                .contains("openlineage_facets")
        );
        assert!(
            report
                .evidence_signal_summary()
                .contains("problem_details_errors")
        );
        assert!(report.evidence_signal_summary().contains("cloudevents"));
        assert!(
            report
                .evidence_signal_summary()
                .contains("certificate_refs")
        );
        assert!(report.credential_references_only);
        assert!(report.secrets_redacted);
        assert!(!report.raw_secret_values_present);
        assert!(!report.credentials_resolved);
        assert!(!report.secret_resolution);
        assert!(!report.effect_policy_violated());
    }

    #[test]
    fn rest_api_security_governance_blocks_destructive_and_keeps_agent_dry_run() {
        let blocked = RestApiSecurityGovernanceReport::for_scenario(
            RestApiSecurityGovernanceScenario::DestructivePolicyRequired,
        );
        let agent = RestApiSecurityGovernanceReport::for_scenario(
            RestApiSecurityGovernanceScenario::AgentMcpDiscovery,
        );

        assert_eq!(blocked.status(), CommandStatus::Unsupported);
        assert_eq!(
            blocked.governance_status,
            RestApiSecurityGovernanceStatus::BlockedPolicyRequired
        );
        assert!(blocked.destructive_operation_requested);
        assert!(blocked.destructive_policy_required);
        assert!(!blocked.destructive_policy_present);
        assert!(!blocked.destructive_operations_allowed);
        assert!(blocked.problem_details_emitted());
        assert_eq!(
            blocked
                .problem_details
                .as_ref()
                .map(|problem| problem.diagnostic_code),
            Some("SL_EXTERNAL_EFFECT_DISABLED")
        );
        assert_eq!(
            blocked.diagnostics[0].code,
            DiagnosticCode::ExternalEffectDisabled
        );
        assert!(!blocked.effect_policy_violated());

        assert_eq!(agent.status(), CommandStatus::Success);
        assert_eq!(
            agent.governance_status,
            RestApiSecurityGovernanceStatus::AgentDryRunOnly
        );
        assert!(agent.mcp_dry_run_default);
        assert!(!agent.mcp_effectful_tools_allowed);
        assert!(agent.mcp_discovery_side_effect_free);
        assert!(!agent.mcp_tool_execution);
        assert!(!agent.fallback_attempted);
        assert!(!agent.effect_policy_violated());
    }

    #[test]
    fn rest_api_data_plane_exposes_transfer_policy_and_decoded_boundary() {
        let report = RestApiDataPlaneReport::for_scenario(
            RestApiDataPlaneScenario::ArtifactReferenceDefault,
        );

        assert_eq!(report.schema_version, REST_API_DATA_PLANE_SCHEMA_VERSION);
        assert_eq!(report.status(), CommandStatus::Success);
        assert_eq!(
            report.data_plane_status,
            RestApiDataPlaneStatus::ContractAvailable
        );
        assert!(report.endpoint_paths.contains(&"/v1/data-plane"));
        assert!(report.rest_control_plane_required);
        assert!(report.rest_control_plane_sufficient_for_local_use);
        assert!(!report.flight_adbc_required_for_basic_local_use);
        assert!(report.transfer_mode_summary().contains("vortex_artifact"));
        assert!(
            report
                .transfer_mode_summary()
                .contains("arrow_ipc_decoded_boundary:decoded_columnar_boundary")
        );
        assert!(report.decoded_columnar_boundary_declared);
        assert!(report.materialization_declared);
        assert!(report.fidelity_declared);
        assert!(report.result_policy_declared);
        assert!(report.vortex_artifact_available);
        assert!(report.object_reference_available);
        assert!(report.arrow_ipc_decoded_boundary_available);
        assert!(!report.arrow_ipc_certified_native);
        assert!(!report.flight_ticket_supported);
        assert!(!report.adbc_endpoint_supported);
        assert!(!report.server_started);
        assert!(!report.network_listener_opened);
        assert!(!report.data_read);
        assert!(!report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.effect_policy_violated());
    }

    #[test]
    fn rest_api_data_plane_marks_flight_adbc_optional_and_lists_standards_boundaries() {
        let flight =
            RestApiDataPlaneReport::for_scenario(RestApiDataPlaneScenario::FlightTicketRequested);
        let adbc =
            RestApiDataPlaneReport::for_scenario(RestApiDataPlaneScenario::AdbcEndpointRequested);
        let standards =
            RestApiDataPlaneReport::for_scenario(RestApiDataPlaneScenario::StandardsMatrix);

        assert_eq!(flight.status(), CommandStatus::Warning);
        assert_eq!(
            flight.data_plane_status,
            RestApiDataPlaneStatus::OptionalTransportPlanned
        );
        assert!(flight.flight_ticket_requested);
        assert!(!flight.flight_ticket_supported);
        assert!(!flight.flight_server_started);
        assert!(!flight.optional_transport_required);
        assert_eq!(flight.diagnostics[0].code, DiagnosticCode::NotImplemented);

        assert_eq!(adbc.status(), CommandStatus::Warning);
        assert!(adbc.adbc_endpoint_requested);
        assert!(!adbc.adbc_endpoint_supported);
        assert!(!adbc.adbc_endpoint_opened);
        assert!(!adbc.optional_transport_required);

        assert_eq!(standards.status(), CommandStatus::Success);
        assert_eq!(
            standards.data_plane_status,
            RestApiDataPlaneStatus::StandardsMatrixAvailable
        );
        assert!(standards.standards_matrix_requested);
        assert_eq!(standards.standards_matrix_count, 11);
        assert!(
            standards
                .standards_name_summary()
                .contains("iceberg_rest_catalog")
        );
        assert!(standards.standards_name_summary().contains("polaris"));
        assert!(standards.standards_name_summary().contains("gravitino"));
        assert!(standards.standards_name_summary().contains("delta_sharing"));
        assert!(standards.standards_name_summary().contains("substrait"));
        assert!(
            standards
                .standards_name_summary()
                .contains("wasi_webassembly_components")
        );
        assert!(
            standards
                .standards_name_summary()
                .contains("nats_jetstream")
        );
        assert!(standards.standards_name_summary().contains("redpanda"));
        assert!(
            standards
                .standards_name_summary()
                .contains("kafka_compatible")
        );
        assert!(standards.standards_name_summary().contains("paimon"));
        assert!(standards.standards_name_summary().contains("fluss"));
        assert!(
            standards
                .standards
                .iter()
                .all(|standard| !standard.execution_allowed && !standard.fallback_allowed)
        );
        assert!(
            standards
                .standards
                .iter()
                .all(|standard| !standard.broker_io && !standard.catalog_io)
        );
        assert!(!standards.effect_policy_violated());
    }

    #[test]
    fn checked_in_openapi_contract_matches_report() {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let contract_path = manifest_dir.join("..").join(OPENAPI_CONTRACT_PATH);
        let contract = fs::read_to_string(&contract_path)
            .unwrap_or_else(|err| panic!("failed to read {contract_path:?}: {err}"));

        assert!(contract.contains("openapi: 3.2.0"));
        assert!(contract.contains("/v1/health:"));
        assert!(contract.contains("/v1/capabilities:"));
        assert!(contract.contains("application/problem+json"));
        assert!(contract.contains("ExecutionRequestPolicy"));
        assert!(contract.contains("ShardLoomExecutionMode"));
        assert!(contract.contains("ExecutionModeSelectionReport"));
        assert_openapi_execution_mode_parity_contract(&contract);
        assert!(contract.contains("PlanPreviewResponse"));
        assert!(contract.contains("/v1/plans/{plan_handle}/certification-preview:"));
        assert!(contract.contains("LocalLifecycleResponse"));
        assert!(contract.contains("/v1/queries/{query_id}/cancel:"));
        assert!(contract.contains("EventStreamResponse"));
        assert!(contract.contains("/v1/events/streams/{stream_id}/sse:"));
        assert!(contract.contains("SecurityGovernanceResponse"));
        assert!(contract.contains("/v1/security/auth:"));
        assert!(contract.contains("/v1/observability/evidence-model:"));
        assert!(contract.contains("/v1/mcp/tools:"));
        assert!(contract.contains("DataPlaneResponse"));
        assert!(contract.contains("/v1/data-plane:"));
        assert!(contract.contains("/v1/results/{result_id}/flight-ticket:"));
        assert!(contract.contains("/v1/results/{result_id}/adbc-endpoint:"));
        assert!(contract.contains("/v1/data-plane/standards:"));
        assert_response_uses_domain_status(&contract, "PlanPreviewResponse", "preview_status");
        assert_response_uses_domain_status(&contract, "LocalLifecycleResponse", "lifecycle_status");
        assert_response_uses_domain_status(&contract, "EventStreamResponse", "event_stream_status");
        assert_response_uses_domain_status(
            &contract,
            "SecurityGovernanceResponse",
            "governance_status",
        );
        assert_response_uses_domain_status(&contract, "DataPlaneResponse", "data_plane_status");

        let asyncapi_path = manifest_dir.join("..").join(ASYNCAPI_EVENT_CONTRACT_PATH);
        let asyncapi = fs::read_to_string(&asyncapi_path)
            .unwrap_or_else(|err| panic!("failed to read {asyncapi_path:?}: {err}"));
        assert!(asyncapi.contains("asyncapi: 3.0.0"));
        assert!(asyncapi.contains("/v1/events/streams/{stream_id}/sse"));
        assert!(asyncapi.contains("CloudEventEnvelope"));
    }

    fn assert_openapi_execution_mode_parity_contract(contract: &str) {
        let mode_block = openapi_schema_block(contract, "ShardLoomExecutionMode");
        for mode in [
            "auto",
            "compatibility_import_certified",
            "prepared_vortex",
            "native_vortex",
            "direct_compatibility_transient",
        ] {
            assert!(
                mode_block.contains(mode),
                "missing REST execution mode {mode}"
            );
        }

        let policy_block = openapi_schema_block(contract, "ExecutionRequestPolicy");
        assert!(policy_block.contains("        - requested_execution_mode"));
        assert!(policy_block.contains("        requested_execution_mode:"));

        let selection_block = openapi_schema_block(contract, "ExecutionModeSelectionReport");
        for field in [
            "requested_execution_mode",
            "selected_execution_mode",
            "mode_selection_reason",
            "support_status",
            "unsupported_diagnostic_code",
            "blocker_id",
            "required_future_evidence",
            "claim_gate_status",
            "fallback_attempted",
            "external_engine_invoked",
        ] {
            assert!(
                selection_block.contains(field),
                "missing execution-mode selection field {field}"
            );
        }

        let preview = openapi_schema_block(contract, "PlanPreviewResponse");
        assert!(preview.contains("            - execution_mode_selection"));
        assert!(preview.contains("            execution_mode_selection:"));

        let lifecycle = openapi_schema_block(contract, "LocalLifecycleResponse");
        assert!(lifecycle.contains("            - execution_mode_selection"));
        assert!(lifecycle.contains("            execution_mode_selection:"));
    }

    fn assert_response_uses_domain_status(contract: &str, schema: &str, field: &str) {
        let block = openapi_schema_block(contract, schema);
        assert!(
            block.contains(&format!("            - {field}")),
            "{schema} does not require {field}"
        );
        assert!(
            block.contains(&format!("            {field}:")),
            "{schema} does not define {field}"
        );
        assert!(
            !block.contains("            - status"),
            "{schema} must not require domain-specific status through OutputEnvelope.status"
        );
        assert!(
            !block.contains("            status:"),
            "{schema} must not redefine OutputEnvelope.status with a domain enum"
        );
    }

    fn openapi_schema_block(contract: &str, schema: &str) -> String {
        let marker = format!("    {schema}:");
        let mut block = Vec::new();
        let mut in_block = false;
        for line in contract.lines() {
            if line == marker {
                in_block = true;
                block.push(line.to_string());
                continue;
            }
            if in_block && line.starts_with("    ") && !line.starts_with("     ") {
                break;
            }
            if in_block {
                block.push(line.to_string());
            }
        }
        assert!(in_block, "missing OpenAPI schema block {schema}");
        block.join("\n")
    }
}
