//! Contract-first CG-23 REST/API planning surfaces.
//!
//! The reports in this module describe the remote control-plane contract
//! without starting a server, opening sockets, probing datasets, reading object
//! stores, consulting catalogs, executing plans, or enabling fallback engines.

use crate::{CommandStatus, Diagnostic, DiagnosticSeverity};

pub const REST_API_CONTRACT_SCHEMA_VERSION: &str = "shardloom.rest_api_contract.v1";
pub const REST_API_DISCOVERY_SCHEMA_VERSION: &str = "shardloom.rest_api_discovery_mode.v1";
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
            status: RestApiMaturityStatus::Planned,
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
    }
}
