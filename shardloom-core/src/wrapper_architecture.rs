//! Client, wrapper, SDK, and ecosystem surface contracts.
//!
//! This module is report-only. It defines one protocol/client architecture and
//! many thin wrappers without implementing generated clients, data-plane
//! bridges, DB adapters, orchestration providers, MCP tools, or runtime
//! fallback behavior.

#![allow(clippy::struct_excessive_bools)]

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WrapperMaturityLevel {
    W0DeclaredOnly,
    W1PackageImportSmoke,
    W2SideEffectFreeCapabilityDiscovery,
    W3TypedEnvelopeParsing,
    W4PlanExplainValidate,
    W5ExecuteCertifiedLocalPaths,
    W6ResultDeliveryAndCertificateAccess,
    W7WorkloadCertifiedIntegration,
}

impl WrapperMaturityLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::W0DeclaredOnly => "w0_declared_only",
            Self::W1PackageImportSmoke => "w1_package_import_smoke",
            Self::W2SideEffectFreeCapabilityDiscovery => "w2_side_effect_free_capability_discovery",
            Self::W3TypedEnvelopeParsing => "w3_typed_envelope_parsing",
            Self::W4PlanExplainValidate => "w4_plan_explain_validate",
            Self::W5ExecuteCertifiedLocalPaths => "w5_execute_certified_local_paths",
            Self::W6ResultDeliveryAndCertificateAccess => {
                "w6_result_delivery_and_certificate_access"
            }
            Self::W7WorkloadCertifiedIntegration => "w7_workload_certified_integration",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::W0DeclaredOnly,
            Self::W1PackageImportSmoke,
            Self::W2SideEffectFreeCapabilityDiscovery,
            Self::W3TypedEnvelopeParsing,
            Self::W4PlanExplainValidate,
            Self::W5ExecuteCertifiedLocalPaths,
            Self::W6ResultDeliveryAndCertificateAccess,
            Self::W7WorkloadCertifiedIntegration,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperTransportKind {
    CliSubprocess,
    RestHttp,
    FlightAdbcDataPlane,
    Mock,
    RecordingReplay,
}

impl WrapperTransportKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CliSubprocess => "cli_subprocess",
            Self::RestHttp => "rest_http",
            Self::FlightAdbcDataPlane => "flight_adbc_data_plane",
            Self::Mock => "mock",
            Self::RecordingReplay => "recording_replay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperFamily {
    LanguageSdk,
    PythonEcosystem,
    WorkflowOrchestration,
    RemoteDataPlane,
    Agent,
}

impl WrapperFamily {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LanguageSdk => "language_sdk",
            Self::PythonEcosystem => "python_ecosystem",
            Self::WorkflowOrchestration => "workflow_orchestration",
            Self::RemoteDataPlane => "remote_data_plane",
            Self::Agent => "agent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolSchemaArtifact {
    pub schema_name: &'static str,
    pub status: &'static str,
    pub required_for_wrappers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCoreOperation {
    pub operation: &'static str,
    pub side_effect_free_by_default: bool,
    pub execution_policy_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperRegistryEntry {
    pub wrapper_id: &'static str,
    pub family: WrapperFamily,
    pub planned_package: &'static str,
    pub maturity: WrapperMaturityLevel,
    pub primary_transport: WrapperTransportKind,
    pub materialization_boundary_required: bool,
    pub certificate_access_required: bool,
    pub external_engine_fallback_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperCapabilityReport {
    pub schema_version: &'static str,
    pub wrapper_id: &'static str,
    pub wrapper_version: &'static str,
    pub protocol_version: &'static str,
    pub maturity: WrapperMaturityLevel,
    pub supported_transports: Vec<WrapperTransportKind>,
    pub exposed_fields: Vec<&'static str>,
    pub unavailable_fields: Vec<&'static str>,
    pub materialization_behavior: &'static str,
    pub certificate_access: &'static str,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl WrapperCapabilityReport {
    #[must_use]
    pub fn current_python_cli_client() -> Self {
        Self {
            schema_version: "shardloom.wrapper_capability_report.v1",
            wrapper_id: "python_cli_json_client",
            wrapper_version: "source_tree",
            protocol_version: "shardloom.protocol.v1",
            maturity: WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths,
            supported_transports: vec![WrapperTransportKind::CliSubprocess],
            exposed_fields: vec![
                "output_envelope",
                "diagnostics",
                "fields",
                "fallback_attempted",
                "certificate_refs",
            ],
            unavailable_fields: vec![
                "rest_query_lifecycle",
                "flight_ticket",
                "adbc_stream",
                "remote_cancel",
            ],
            materialization_behavior: "explicit_cli_result_or_artifact_boundary",
            certificate_access: "preserved_from_output_envelope_fields",
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn no_fallback_visible(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperGoldenContractFixture {
    pub fixture_id: &'static str,
    pub artifact_kind: &'static str,
    pub required_before_maturity: WrapperMaturityLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperGoldenContractFixtureCatalog {
    pub schema_version: &'static str,
    pub fixtures: Vec<WrapperGoldenContractFixture>,
}

impl WrapperGoldenContractFixtureCatalog {
    #[must_use]
    pub fn required() -> Self {
        let fixture = |fixture_id, artifact_kind| WrapperGoldenContractFixture {
            fixture_id,
            artifact_kind,
            required_before_maturity: WrapperMaturityLevel::W3TypedEnvelopeParsing,
        };
        Self {
            schema_version: "shardloom.wrapper_golden_contract_fixture_catalog.v1",
            fixtures: vec![
                fixture("golden.output_envelope.success", "output_envelope"),
                fixture("golden.output_envelope.unsupported", "unsupported_error"),
                fixture("golden.capabilities.snapshot", "capability_snapshot"),
                fixture("golden.result_ref.artifact", "result_ref"),
                fixture(
                    "golden.materialization_report.explicit_boundary",
                    "materialization_report",
                ),
                fixture(
                    "golden.execution_certificate.certified",
                    "execution_certificate",
                ),
                fixture(
                    "golden.native_io_certificate.certified",
                    "native_io_certificate",
                ),
            ],
        }
    }

    #[must_use]
    pub fn artifact_kinds(&self) -> Vec<&'static str> {
        self.fixtures
            .iter()
            .map(|fixture| fixture.artifact_kind)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientWrapperArchitectureReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub architecture_layers: Vec<&'static str>,
    pub protocol_schema_artifacts: Vec<ProtocolSchemaArtifact>,
    pub transport_adapters: Vec<WrapperTransportKind>,
    pub client_core_operations: Vec<ClientCoreOperation>,
    pub language_sdk_registry: Vec<WrapperRegistryEntry>,
    pub python_ecosystem_registry: Vec<WrapperRegistryEntry>,
    pub workflow_registry: Vec<WrapperRegistryEntry>,
    pub remote_data_plane_registry: Vec<WrapperRegistryEntry>,
    pub agent_registry: Vec<WrapperRegistryEntry>,
    pub wrapper_capability_report: WrapperCapabilityReport,
    pub golden_contract_fixtures: WrapperGoldenContractFixtureCatalog,
    pub import_side_effects_allowed: bool,
    pub client_construction_dataset_probe_allowed: bool,
    pub unsupported_behavior_structured: bool,
    pub large_results_use_refs_or_columnar_boundaries: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ClientWrapperArchitectureReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.client_wrapper_architecture.v1",
            report_id: "priority_3_8.client_wrapper_architecture",
            architecture_layers: vec![
                "protocol_schemas",
                "transport_adapters",
                "client_core",
                "language_sdks",
                "ecosystem_wrappers",
            ],
            protocol_schema_artifacts: protocol_schema_artifacts(),
            transport_adapters: vec![
                WrapperTransportKind::CliSubprocess,
                WrapperTransportKind::RestHttp,
                WrapperTransportKind::FlightAdbcDataPlane,
                WrapperTransportKind::Mock,
                WrapperTransportKind::RecordingReplay,
            ],
            client_core_operations: client_core_operations(),
            language_sdk_registry: language_sdk_registry(),
            python_ecosystem_registry: python_ecosystem_registry(),
            workflow_registry: workflow_registry(),
            remote_data_plane_registry: remote_data_plane_registry(),
            agent_registry: agent_registry(),
            wrapper_capability_report: WrapperCapabilityReport::current_python_cli_client(),
            golden_contract_fixtures: WrapperGoldenContractFixtureCatalog::required(),
            import_side_effects_allowed: false,
            client_construction_dataset_probe_allowed: false,
            unsupported_behavior_structured: true,
            large_results_use_refs_or_columnar_boundaries: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn wrapper_families_count(&self) -> usize {
        [
            &self.language_sdk_registry,
            &self.python_ecosystem_registry,
            &self.workflow_registry,
            &self.remote_data_plane_registry,
            &self.agent_registry,
        ]
        .iter()
        .map(|registry| registry.len())
        .sum()
    }

    #[must_use]
    pub fn preserves_wrapper_invariants(&self) -> bool {
        !self.import_side_effects_allowed
            && !self.client_construction_dataset_probe_allowed
            && self.unsupported_behavior_structured
            && self.large_results_use_refs_or_columnar_boundaries
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && self.wrapper_capability_report.no_fallback_visible()
            && all_registry_entries_disable_fallback(&self.language_sdk_registry)
            && all_registry_entries_disable_fallback(&self.python_ecosystem_registry)
            && all_registry_entries_disable_fallback(&self.workflow_registry)
            && all_registry_entries_disable_fallback(&self.remote_data_plane_registry)
            && all_registry_entries_disable_fallback(&self.agent_registry)
    }
}

#[must_use]
pub fn plan_client_wrapper_architecture() -> ClientWrapperArchitectureReport {
    ClientWrapperArchitectureReport::current()
}

fn protocol_schema_artifacts() -> Vec<ProtocolSchemaArtifact> {
    [
        "OutputEnvelope",
        "CapabilitySnapshot",
        "ExecutionCertificate",
        "NativeIoCertificate",
        "EvidenceArtifactEnvelope",
        "ShardLoomExecutionPolicy",
        "ResultRef",
        "ProblemDetails",
        "EngineSelectionReport",
        "MaterializationBoundaryReport",
        "AdapterFidelityReport",
        "BenchmarkClaimEvidenceReport",
    ]
    .iter()
    .map(|schema_name| ProtocolSchemaArtifact {
        schema_name,
        status: "contract_required",
        required_for_wrappers: true,
    })
    .collect()
}

fn client_core_operations() -> Vec<ClientCoreOperation> {
    [
        ("status", true, false),
        ("capabilities", true, false),
        ("adapter_discovery", true, false),
        ("plan_validation", true, false),
        ("explain", true, false),
        ("execute", false, true),
        ("query_status", true, false),
        ("cancel", false, true),
        ("results", false, true),
        ("certificates", true, false),
        ("profile", true, false),
        ("benchmark", false, true),
        ("migration", true, false),
        ("diagnostics", true, false),
    ]
    .iter()
    .map(
        |(operation, side_effect_free_by_default, execution_policy_required)| ClientCoreOperation {
            operation,
            side_effect_free_by_default: *side_effect_free_by_default,
            execution_policy_required: *execution_policy_required,
        },
    )
    .collect()
}

fn entry(
    wrapper_id: &'static str,
    family: WrapperFamily,
    planned_package: &'static str,
    maturity: WrapperMaturityLevel,
    primary_transport: WrapperTransportKind,
) -> WrapperRegistryEntry {
    WrapperRegistryEntry {
        wrapper_id,
        family,
        planned_package,
        maturity,
        primary_transport,
        materialization_boundary_required: true,
        certificate_access_required: true,
        external_engine_fallback_allowed: false,
    }
}

fn language_sdk_registry() -> Vec<WrapperRegistryEntry> {
    vec![
        entry(
            "python",
            WrapperFamily::LanguageSdk,
            "shardloom-python",
            WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths,
            WrapperTransportKind::CliSubprocess,
        ),
        entry(
            "rust",
            WrapperFamily::LanguageSdk,
            "shardloom-client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
        ),
        entry(
            "typescript_javascript",
            WrapperFamily::LanguageSdk,
            "shardloom-js",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "go",
            WrapperFamily::LanguageSdk,
            "shardloom-go",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "java_jvm",
            WrapperFamily::LanguageSdk,
            "shardloom-java-client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "dotnet",
            WrapperFamily::LanguageSdk,
            "ShardLoom.Client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "r",
            WrapperFamily::LanguageSdk,
            "shardloomr",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
    ]
}

fn python_ecosystem_registry() -> Vec<WrapperRegistryEntry> {
    vec![
        entry(
            "python_dbapi",
            WrapperFamily::PythonEcosystem,
            "shardloom-dbapi",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "sqlalchemy",
            WrapperFamily::PythonEcosystem,
            "sqlalchemy-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "ibis",
            WrapperFamily::PythonEcosystem,
            "ibis-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "pandas_arrow_helpers",
            WrapperFamily::PythonEcosystem,
            "shardloom-python",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
        ),
        entry(
            "notebook_display",
            WrapperFamily::PythonEcosystem,
            "shardloom-python",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
        ),
    ]
}

fn workflow_registry() -> Vec<WrapperRegistryEntry> {
    vec![
        entry(
            "dbt",
            WrapperFamily::WorkflowOrchestration,
            "dbt-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "airflow",
            WrapperFamily::WorkflowOrchestration,
            "apache-airflow-providers-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "dagster",
            WrapperFamily::WorkflowOrchestration,
            "dagster-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "prefect",
            WrapperFamily::WorkflowOrchestration,
            "prefect-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "ci_report_viewer",
            WrapperFamily::WorkflowOrchestration,
            "shardloom-reports",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
        ),
    ]
}

fn remote_data_plane_registry() -> Vec<WrapperRegistryEntry> {
    vec![
        entry(
            "adbc",
            WrapperFamily::RemoteDataPlane,
            "shardloom-adbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
        ),
        entry(
            "flight_sql",
            WrapperFamily::RemoteDataPlane,
            "shardloom-flight-sql",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
        ),
        entry(
            "jdbc_via_flight_sql",
            WrapperFamily::RemoteDataPlane,
            "arrow-flight-sql-jdbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
        ),
        entry(
            "odbc_later",
            WrapperFamily::RemoteDataPlane,
            "shardloom-odbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
        ),
        entry(
            "superset_bi_readiness",
            WrapperFamily::RemoteDataPlane,
            "sqlalchemy-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
        entry(
            "grafana_datasource",
            WrapperFamily::RemoteDataPlane,
            "grafana-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
        ),
    ]
}

fn agent_registry() -> Vec<WrapperRegistryEntry> {
    vec![entry(
        "mcp",
        WrapperFamily::Agent,
        "shardloom-mcp",
        WrapperMaturityLevel::W0DeclaredOnly,
        WrapperTransportKind::RestHttp,
    )]
}

fn all_registry_entries_disable_fallback(entries: &[WrapperRegistryEntry]) -> bool {
    entries
        .iter()
        .all(|entry| !entry.external_engine_fallback_allowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapper_architecture_preserves_one_protocol_many_thin_wrappers() {
        let report = plan_client_wrapper_architecture();

        assert_eq!(
            report.architecture_layers,
            vec![
                "protocol_schemas",
                "transport_adapters",
                "client_core",
                "language_sdks",
                "ecosystem_wrappers"
            ]
        );
        assert!(report.preserves_wrapper_invariants());
        assert_eq!(WrapperMaturityLevel::all().len(), 8);
    }

    #[test]
    fn schema_and_transport_contracts_cover_expected_surfaces() {
        let report = plan_client_wrapper_architecture();
        let schemas = report
            .protocol_schema_artifacts
            .iter()
            .map(|schema| schema.schema_name)
            .collect::<Vec<_>>();

        assert!(schemas.contains(&"OutputEnvelope"));
        assert!(schemas.contains(&"EvidenceArtifactEnvelope"));
        assert!(schemas.contains(&"ShardLoomExecutionPolicy"));
        assert!(
            report
                .transport_adapters
                .contains(&WrapperTransportKind::CliSubprocess)
        );
        assert!(
            report
                .transport_adapters
                .contains(&WrapperTransportKind::RestHttp)
        );
        assert!(
            report
                .transport_adapters
                .contains(&WrapperTransportKind::FlightAdbcDataPlane)
        );
    }

    #[test]
    fn registries_keep_future_wrappers_declared_and_fallback_free() {
        let report = plan_client_wrapper_architecture();

        assert!(report.wrapper_families_count() >= 24);
        assert!(
            report
                .language_sdk_registry
                .iter()
                .any(|entry| entry.wrapper_id == "python"
                    && entry.maturity == WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths)
        );
        assert!(
            report
                .python_ecosystem_registry
                .iter()
                .any(|entry| entry.wrapper_id == "sqlalchemy")
        );
        assert!(
            report
                .agent_registry
                .iter()
                .any(|entry| entry.wrapper_id == "mcp")
        );
        assert!(all_registry_entries_disable_fallback(
            &report.workflow_registry
        ));
        assert!(all_registry_entries_disable_fallback(
            &report.remote_data_plane_registry
        ));
    }

    #[test]
    fn wrapper_capability_and_golden_fixture_catalog_preserve_certificate_truth() {
        let report = plan_client_wrapper_architecture();

        assert_eq!(
            report.wrapper_capability_report.wrapper_id,
            "python_cli_json_client"
        );
        assert_eq!(
            report.wrapper_capability_report.maturity,
            WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths
        );
        assert!(report.wrapper_capability_report.no_fallback_visible());
        assert!(
            report
                .golden_contract_fixtures
                .artifact_kinds()
                .contains(&"execution_certificate")
        );
        assert!(
            report
                .golden_contract_fixtures
                .artifact_kinds()
                .contains(&"native_io_certificate")
        );
    }
}
