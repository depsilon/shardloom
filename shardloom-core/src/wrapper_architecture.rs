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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperConnectorSupportStatus {
    ReadyLocal,
    ReportOnly,
    Blocked,
}

impl WrapperConnectorSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadyLocal => "ready_local",
            Self::ReportOnly => "report_only",
            Self::Blocked => "blocked",
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
pub struct WrapperConnectorRegistryRow {
    pub row_id: &'static str,
    pub family: WrapperFamily,
    pub planned_package: &'static str,
    pub maturity: WrapperMaturityLevel,
    pub primary_transport: WrapperTransportKind,
    pub support_status: WrapperConnectorSupportStatus,
    pub user_visible_surface: &'static str,
    pub implementation_evidence: &'static str,
    pub deterministic_diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub explicit_execution_available: bool,
    pub dependency_added: bool,
    pub network_listener_started: bool,
    pub data_plane_bridge_supported: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
}

impl WrapperConnectorRegistryRow {
    #[must_use]
    pub const fn no_fallback_no_external_engine(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperConnectorImplementationRegistryReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub docs_ref: &'static str,
    pub support_status_vocabulary: &'static str,
    pub rows: Vec<WrapperConnectorRegistryRow>,
    pub dependency_expansion_allowed: bool,
    pub wrapper_ecosystem_claim_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl WrapperConnectorImplementationRegistryReport {
    #[must_use]
    pub fn gar0037a_current() -> Self {
        Self {
            schema_version: "shardloom.wrapper_connector_implementation_registry.v1",
            report_id: "gar-0037-a.wrapper_connector_implementation_registry",
            docs_ref: "docs/architecture/wrapper-connector-implementation-registry.md",
            support_status_vocabulary: "ready_local,report_only,blocked",
            rows: wrapper_connector_registry_rows(),
            dependency_expansion_allowed: false,
            wrapper_ecosystem_claim_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn ready_local_count(&self) -> usize {
        self.status_count(WrapperConnectorSupportStatus::ReadyLocal)
    }

    #[must_use]
    pub fn report_only_count(&self) -> usize {
        self.status_count(WrapperConnectorSupportStatus::ReportOnly)
    }

    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.status_count(WrapperConnectorSupportStatus::Blocked)
    }

    #[must_use]
    pub fn diagnostic_codes(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.deterministic_diagnostic_code)
            .collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn all_rows_no_fallback_no_external_engine(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(WrapperConnectorRegistryRow::no_fallback_no_external_engine)
    }

    fn status_count(&self, status: WrapperConnectorSupportStatus) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == status)
            .count()
    }
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

#[allow(clippy::too_many_arguments)]
fn registry_row(
    row_id: &'static str,
    family: WrapperFamily,
    planned_package: &'static str,
    maturity: WrapperMaturityLevel,
    primary_transport: WrapperTransportKind,
    support_status: WrapperConnectorSupportStatus,
    user_visible_surface: &'static str,
    implementation_evidence: &'static str,
    deterministic_diagnostic_code: &'static str,
    required_evidence: &'static str,
    explicit_execution_available: bool,
    data_plane_bridge_supported: bool,
    claim_boundary: &'static str,
) -> WrapperConnectorRegistryRow {
    WrapperConnectorRegistryRow {
        row_id,
        family,
        planned_package,
        maturity,
        primary_transport,
        support_status,
        user_visible_surface,
        implementation_evidence,
        deterministic_diagnostic_code,
        required_evidence,
        explicit_execution_available,
        dependency_added: false,
        network_listener_started: false,
        data_plane_bridge_supported,
        external_engine_invoked: false,
        fallback_attempted: false,
        claim_gate_status: "not_claim_grade",
        claim_boundary,
    }
}

#[allow(clippy::too_many_lines)]
fn wrapper_connector_registry_rows() -> Vec<WrapperConnectorRegistryRow> {
    let ready = WrapperConnectorSupportStatus::ReadyLocal;
    let report_only = WrapperConnectorSupportStatus::ReportOnly;
    let blocked = WrapperConnectorSupportStatus::Blocked;
    vec![
        registry_row(
            "python_cli_json_client",
            WrapperFamily::LanguageSdk,
            "shardloom-python",
            WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths,
            WrapperTransportKind::CliSubprocess,
            ready,
            "python.src.shardloom.client",
            "source_tree_python_client,cli_json_envelope,python_client_tests",
            "none_supported_local_cli_json_wrapper",
            "output_envelope,diagnostics,fields,fallback_policy,certificate_refs",
            true,
            false,
            "Ready local Python wrapper over explicit ShardLoom CLI JSON; it is not a separate engine, REST server, generated client ecosystem, or production wrapper claim.",
        ),
        registry_row(
            "python_typed_capability_views",
            WrapperFamily::LanguageSdk,
            "shardloom-python",
            WrapperMaturityLevel::W3TypedEnvelopeParsing,
            WrapperTransportKind::CliSubprocess,
            ready,
            "python.src.shardloom.context",
            "capability_posture_views,typed_matrix_accessors,python_tests",
            "none_supported_typed_capability_views",
            "capability_snapshot,claim_gate_status,no_fallback_fields,unsupported_diagnostics",
            false,
            false,
            "Ready local typed view over existing capability reports; it does not create new runtime support or package-channel readiness.",
        ),
        registry_row(
            "python_generated_source_helpers",
            WrapperFamily::LanguageSdk,
            "shardloom-python",
            WrapperMaturityLevel::W5ExecuteCertifiedLocalPaths,
            WrapperTransportKind::CliSubprocess,
            ready,
            "ctx.from_rows(...).write(...),ctx.range(...).write(...)",
            "scoped_local_generated_source_smokes,generated_source_certificate_fields",
            "none_scoped_local_generated_source_smoke_only",
            "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            true,
            false,
            "Scoped local generated-source smoke helpers only; not broad SQL/DataFrame generated-output runtime, object-store output, or production API support.",
        ),
        registry_row(
            "rust_client",
            WrapperFamily::LanguageSdk,
            "shardloom-client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
            report_only,
            "future Rust client crate",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "package_metadata,protocol_fixtures,typed_envelope_parser,no_fallback_policy",
            false,
            false,
            "Rust client remains declared/report-only and cannot be advertised as implemented.",
        ),
        registry_row(
            "typescript_javascript_client",
            WrapperFamily::LanguageSdk,
            "shardloom-js",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "future TypeScript/JavaScript client",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "openapi_contract,generated_client_fixture,rest_runtime_proof,no_fallback_policy",
            false,
            false,
            "Generated JS client remains report-only because no REST runtime or generated client package exists.",
        ),
        registry_row(
            "go_client",
            WrapperFamily::LanguageSdk,
            "shardloom-go",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "future Go client",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "openapi_contract,generated_client_fixture,rest_runtime_proof,no_fallback_policy",
            false,
            false,
            "Go client remains report-only because no REST runtime or generated client package exists.",
        ),
        registry_row(
            "java_jvm_client",
            WrapperFamily::LanguageSdk,
            "shardloom-java-client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "future JVM client",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "openapi_contract,generated_client_fixture,rest_runtime_proof,no_fallback_policy",
            false,
            false,
            "JVM client remains report-only and cannot imply JDBC, Spark, or production server support.",
        ),
        registry_row(
            "dotnet_client",
            WrapperFamily::LanguageSdk,
            "ShardLoom.Client",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "future .NET client",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "openapi_contract,generated_client_fixture,rest_runtime_proof,no_fallback_policy",
            false,
            false,
            ".NET client remains report-only because no REST runtime or generated client package exists.",
        ),
        registry_row(
            "r_client",
            WrapperFamily::LanguageSdk,
            "shardloomr",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "future R client",
            "rfc_0037_architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "openapi_contract,generated_client_fixture,rest_runtime_proof,no_fallback_policy",
            false,
            false,
            "R client remains report-only because no REST runtime or generated client package exists.",
        ),
        registry_row(
            "rest_openapi_generated_client",
            WrapperFamily::RemoteDataPlane,
            "generated-openapi-clients",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            report_only,
            "OpenAPI generated clients",
            "rest_contract_plan_report_only",
            "SL_REST_SERVER_UNSUPPORTED",
            "openapi_contract,http_listener_runtime,remote_execution_proof,no_fallback_policy",
            false,
            false,
            "OpenAPI client generation remains report-only while the REST server/runtime is unsupported.",
        ),
        registry_row(
            "ci_report_viewer",
            WrapperFamily::WorkflowOrchestration,
            "shardloom-reports",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
            report_only,
            "future static report viewer",
            "architecture_only",
            "SL_WRAPPER_REPORT_ONLY",
            "static_artifact_schema,read_only_report_fixture,no_fallback_policy",
            false,
            false,
            "Report viewer remains report-only and cannot imply execution, data-plane, or benchmark claims.",
        ),
        registry_row(
            "foundry_transform_wrapper",
            WrapperFamily::WorkflowOrchestration,
            "shardloom-foundry",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::CliSubprocess,
            report_only,
            "local Foundry-style transform wrapper",
            "foundry_local_proof_docs_only",
            "SL_FOUNDRY_PRODUCTION_UNSUPPORTED",
            "foundry_runtime_proof,evidence_dataset_output,no_spark_invocation,no_fallback_policy",
            false,
            false,
            "Foundry wrapper remains local proof/report-only and cannot imply Foundry production support, marketplace publication, or Spark-backed execution.",
        ),
        registry_row(
            "python_dbapi",
            WrapperFamily::PythonEcosystem,
            "shardloom-dbapi",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Python DB-API",
            "not_implemented",
            "SL_DBAPI_CONNECTOR_UNSUPPORTED",
            "dbapi_contract,query_lifecycle,remote_execution_or_local_session_proof,no_fallback_policy",
            false,
            false,
            "DB-API connector is blocked; it is not a fallback engine, SQL production surface, or query-pushdown runtime.",
        ),
        registry_row(
            "sqlalchemy",
            WrapperFamily::PythonEcosystem,
            "sqlalchemy-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "SQLAlchemy dialect",
            "not_implemented",
            "SL_SQLALCHEMY_CONNECTOR_UNSUPPORTED",
            "dialect_contract,sql_parser_binder_runtime,remote_execution_proof,no_fallback_policy",
            false,
            false,
            "SQLAlchemy dialect is blocked and cannot imply production SQL/DataFrame support.",
        ),
        registry_row(
            "ibis",
            WrapperFamily::PythonEcosystem,
            "ibis-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Ibis backend",
            "not_implemented",
            "SL_IBIS_CONNECTOR_UNSUPPORTED",
            "ibis_backend_contract,plan_translation,execution_certificate,no_fallback_policy",
            false,
            false,
            "Ibis backend is blocked; it is not an independent execution engine or production query API.",
        ),
        registry_row(
            "dbt",
            WrapperFamily::WorkflowOrchestration,
            "dbt-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "dbt adapter",
            "not_implemented",
            "SL_DBT_ADAPTER_UNSUPPORTED",
            "adapter_contract,model_execution_proof,materialization_boundary,no_fallback_policy",
            false,
            false,
            "dbt adapter is blocked and cannot imply warehouse, SQL, or production orchestration support.",
        ),
        registry_row(
            "airflow",
            WrapperFamily::WorkflowOrchestration,
            "apache-airflow-providers-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Airflow provider",
            "not_implemented",
            "SL_AIRFLOW_PROVIDER_UNSUPPORTED",
            "operator_contract,task_idempotency,evidence_artifact,no_fallback_policy",
            false,
            false,
            "Airflow provider is blocked and cannot imply managed scheduler, retries, or production pipeline support.",
        ),
        registry_row(
            "dagster",
            WrapperFamily::WorkflowOrchestration,
            "dagster-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Dagster integration",
            "not_implemented",
            "SL_DAGSTER_INTEGRATION_UNSUPPORTED",
            "asset_contract,evidence_io_manager,no_fallback_policy",
            false,
            false,
            "Dagster integration is blocked and cannot imply orchestration runtime support.",
        ),
        registry_row(
            "prefect",
            WrapperFamily::WorkflowOrchestration,
            "prefect-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Prefect integration",
            "not_implemented",
            "SL_PREFECT_INTEGRATION_UNSUPPORTED",
            "task_contract,evidence_artifact,no_fallback_policy",
            false,
            false,
            "Prefect integration is blocked and cannot imply orchestration runtime support.",
        ),
        registry_row(
            "mcp",
            WrapperFamily::Agent,
            "shardloom-mcp",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "MCP resources/tools",
            "not_implemented",
            "SL_MCP_WRAPPER_UNSUPPORTED",
            "resource_contract,tool_contract,permission_policy,no_fallback_policy",
            false,
            false,
            "MCP wrapper is blocked and cannot imply agent tool execution, server runtime, or external effects.",
        ),
        registry_row(
            "flight_sql",
            WrapperFamily::RemoteDataPlane,
            "shardloom-flight-sql",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
            blocked,
            "Flight SQL service",
            "not_implemented",
            "SL_COLUMNAR_TRANSPORT_UNSUPPORTED",
            "flight_server_runtime,ticket_lifecycle,columnar_stream_certificate,no_fallback_policy",
            false,
            false,
            "Flight SQL is blocked; no server, ticket lifecycle, or columnar data-plane runtime exists.",
        ),
        registry_row(
            "adbc",
            WrapperFamily::RemoteDataPlane,
            "shardloom-adbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
            blocked,
            "ADBC driver",
            "not_implemented",
            "SL_ADBC_DRIVER_UNSUPPORTED",
            "adbc_driver_contract,flight_transport_runtime,columnar_stream_certificate,no_fallback_policy",
            false,
            false,
            "ADBC driver is blocked and cannot imply data-plane runtime or BI connector support.",
        ),
        registry_row(
            "jdbc_via_flight_sql",
            WrapperFamily::RemoteDataPlane,
            "arrow-flight-sql-jdbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
            blocked,
            "JDBC via Flight SQL",
            "not_implemented",
            "SL_JDBC_CONNECTOR_UNSUPPORTED",
            "flight_sql_runtime,jdbc_driver_boundary,sql_runtime_claim_gate,no_fallback_policy",
            false,
            false,
            "JDBC is blocked and cannot imply production SQL, BI, or server support.",
        ),
        registry_row(
            "odbc",
            WrapperFamily::RemoteDataPlane,
            "shardloom-odbc",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::FlightAdbcDataPlane,
            blocked,
            "ODBC driver",
            "not_implemented",
            "SL_ODBC_CONNECTOR_UNSUPPORTED",
            "odbc_driver_boundary,flight_sql_runtime,sql_runtime_claim_gate,no_fallback_policy",
            false,
            false,
            "ODBC is blocked and cannot imply BI or production SQL support.",
        ),
        registry_row(
            "bi_connector",
            WrapperFamily::RemoteDataPlane,
            "sqlalchemy-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "BI connector readiness",
            "not_implemented",
            "SL_BI_CONNECTOR_UNSUPPORTED",
            "bi_tool_contract,sql_runtime_claim_gate,result_materialization_boundary,no_fallback_policy",
            false,
            false,
            "BI connector readiness is blocked and cannot imply dashboard, server, or production SQL support.",
        ),
        registry_row(
            "grafana_datasource",
            WrapperFamily::RemoteDataPlane,
            "grafana-shardloom",
            WrapperMaturityLevel::W0DeclaredOnly,
            WrapperTransportKind::RestHttp,
            blocked,
            "Grafana datasource",
            "not_implemented",
            "SL_GRAFANA_DATASOURCE_UNSUPPORTED",
            "datasource_contract,server_runtime,query_lifecycle,no_fallback_policy",
            false,
            false,
            "Grafana datasource is blocked and cannot imply live API, server, or production dashboard support.",
        ),
    ]
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

    #[test]
    fn wrapper_connector_registry_classifies_implemented_and_blocked_surfaces() {
        let report = WrapperConnectorImplementationRegistryReport::gar0037a_current();

        assert_eq!(
            report.schema_version,
            "shardloom.wrapper_connector_implementation_registry.v1"
        );
        assert_eq!(
            report.report_id,
            "gar-0037-a.wrapper_connector_implementation_registry"
        );
        assert_eq!(report.ready_local_count(), 3);
        assert_eq!(report.report_only_count(), 9);
        assert_eq!(report.blocked_count(), 14);
        assert!(!report.dependency_expansion_allowed);
        assert!(!report.wrapper_ecosystem_claim_allowed);
        assert!(report.all_rows_no_fallback_no_external_engine());
        assert!(report.row_order().contains(&"python_cli_json_client"));
        assert!(report.row_order().contains(&"sqlalchemy"));
        assert!(report.row_order().contains(&"flight_sql"));
        assert!(
            report
                .diagnostic_codes()
                .contains(&"SL_COLUMNAR_TRANSPORT_UNSUPPORTED")
        );
        assert!(report.required_evidence().contains(
            &"openapi_contract,http_listener_runtime,remote_execution_proof,no_fallback_policy"
        ));
        assert!(
            report
                .rows
                .iter()
                .filter(|row| row.support_status == WrapperConnectorSupportStatus::Blocked)
                .all(|row| !row.data_plane_bridge_supported)
        );
    }
}
