//! Contract-first CG-23 REST/API planning surfaces.
//!
//! The reports in this module describe the remote control-plane contract
//! without starting a server, opening sockets, probing datasets, reading object
//! stores, consulting catalogs, executing plans, or enabling fallback engines.

use crate::{
    CommandStatus, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus,
};

pub const REST_API_CONTRACT_SCHEMA_VERSION: &str = "shardloom.rest_api_contract.v1";
pub const REST_API_DISCOVERY_SCHEMA_VERSION: &str = "shardloom.rest_api_discovery_mode.v1";
pub const REST_API_PLAN_PREVIEW_SCHEMA_VERSION: &str = "shardloom.rest_api_plan_preview.v1";
pub const OPENAPI_CONTRACT_PATH: &str = "docs/api/shardloom-openapi-v1.yaml";
pub const OPENAPI_VERSION: &str = "3.2.0";
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
                "certificates",
                "profiles",
                "benchmarks",
                "migration",
                "lineage",
                "governance",
            ],
            execution_policy_fields: vec![
                "engine_mode",
                "fallback_policy",
                "materialization_policy",
                "result_policy",
                "evidence_policy",
            ],
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
            "rest api contract\nschema_version: {}\nreport: {}\napi version: {}\nopenapi: {}\ncontract: {}\nresources: {}\ndiscovery endpoints: {}\nproblem details: {}\nserver started: false\nnetwork listener: false\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.api_version,
            self.openapi_version,
            self.openapi_contract_path,
            self.represented_resources.join(", "),
            self.endpoint_paths().join(", "),
            self.problem_details_media_type,
        )
    }
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
            native_logical_planned: !matches!(scenario, RestApiPlanPreviewScenario::InvalidInput),
            native_physical_planned: matches!(
                scenario,
                RestApiPlanPreviewScenario::CertifiedLocalBatch
                    | RestApiPlanPreviewScenario::PartialHybridFixture
            ),
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
            status: RestApiMaturityStatus::BlockedUntilEvidence,
            server_required: true,
            execution_capable: true,
        },
        RestApiMaturityStage {
            stage_id: "API-A5",
            label: "result_delivery_and_spooling",
            status: RestApiMaturityStatus::Planned,
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
            status: RestApiMaturityStatus::BlockedUntilEvidence,
            server_required: true,
            execution_capable: true,
        },
        RestApiMaturityStage {
            stage_id: "API-A8",
            label: "security_governance_quotas_audit",
            status: RestApiMaturityStatus::Planned,
            server_required: true,
            execution_capable: false,
        },
        RestApiMaturityStage {
            stage_id: "API-A9",
            label: "production_certified_workload_api",
            status: RestApiMaturityStatus::BlockedUntilEvidence,
            server_required: true,
            execution_capable: true,
        },
    ]
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
        assert!(report.execution_policy_fields.contains(&"engine_mode"));
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
    fn rest_api_discovery_mode_contract_does_not_start_listener() {
        let report = RestApiDiscoveryModeReport::contract_only("127.0.0.1:8787");

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
        assert!(!invalid.effect_policy_violated());
        assert!(!unsupported.effect_policy_violated());
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
        assert!(contract.contains("PlanPreviewResponse"));
        assert!(contract.contains("/v1/plans/{plan_handle}/certification-preview:"));
    }
}
