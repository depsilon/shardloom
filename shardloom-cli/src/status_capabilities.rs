//! Status and capability-discovery CLI handlers.
//!
//! This is the first physical command-family handler split for Priority 3.9.
//! It keeps behavior identical to the old `main.rs` match arms while routing
//! output through the shared typed-envelope renderer.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{
    CapabilityCertificationReport, CapabilityCertificationStatus, CommandStatus,
    EngineCapabilities, EngineCapabilityMatrixReport, OutputFormat, PhysicalOperatorExecutionLevel,
    PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorPlan, ShardLoomError,
    WorldClassSufficiencyDimensionKind, WorldClassSufficiencyReport, boundedness_vocabulary,
    engine_mode_vocabulary, output_mode_vocabulary, plan_world_class_sufficiency,
    update_mode_vocabulary,
};
use shardloom_vortex::{
    vortex_encoded_count_local_guard_discovery_report,
    vortex_encoded_count_physical_kernel_discovery_report,
    vortex_encoded_predicate_evaluation_discovery_report,
    vortex_selection_vector_filter_kernel_discovery_report,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const WORKFLOW_OPERATION_NAMES: &str = "profile,collect,to_pandas,to_arrow,write_vortex,write_parquet,sql,join,aggregate,window,schema_contract,data_quality";
const WORKFLOW_BLOCKER_IDS: &str = concat!(
    "cg21.workflow.profile.runtime_profile_unsupported,",
    "cg21.workflow.collect.materialization_unsupported,",
    "cg21.workflow.to_pandas.decoded_dataframe_unsupported,",
    "cg21.workflow.to_arrow.decoded_columnar_unsupported,",
    "cg21.workflow.write_vortex.write_policy_unsupported,",
    "cg21.workflow.write_parquet.compatibility_export_unsupported,",
    "cg21.workflow.sql.frontend_unsupported,",
    "cg21.workflow.join.operator_unsupported,",
    "cg21.workflow.aggregate.operator_unsupported,",
    "cg21.workflow.window.operator_unsupported,",
    "cg21.workflow.schema_contract.enforcement_unsupported,",
    "cg21.workflow.data_quality.checks_unsupported"
);
const WORKFLOW_REQUIRED_EVIDENCE: &str = "execution_certificate,native_io_certificate,operator_capability_matrix,write_intent,rest_api_contract";
const WORKFLOW_SUGGESTED_NEXT_ACTION: &str = "Use workflow-unsupported-plan for method-specific blocker details before requesting execution.";
const REMOTE_API_BLOCKER_IDS: &str = concat!(
    "cg23.remote_api.plan_preview.unsupported_operator,",
    "cg23.remote_api.remote_object_store.unsupported,",
    "cg23.remote_api.lifecycle.uncertified_blocked,",
    "cg23.remote_api.data_plane.materialization_boundary_required"
);
const REMOTE_API_REQUIRED_EVIDENCE: &str = "openapi_contract,asyncapi_contract,execution_certificate,native_io_certificate,security_governance_policy,data_plane_fidelity_report";
const REMOTE_API_SUGGESTED_NEXT_ACTION: &str = "Use rest-api-contract-plan and rest-api-plan-preview for scenario-specific blockers before enabling remote execution.";

pub(crate) fn handle_status(format: OutputFormat) -> ExitCode {
    let status = shardloom_exec::status();
    emit(
        "status",
        format,
        CommandStatus::Success,
        "engine status".to_string(),
        format!("{}\nfallback execution: disabled", status.summary),
        vec![],
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "cli_binary_version".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ),
            (
                "protocol_version".to_string(),
                "shardloom.output.v2".to_string(),
            ),
            ("platform_os".to_string(), std::env::consts::OS.to_string()),
            (
                "platform_arch".to_string(),
                std::env::consts::ARCH.to_string(),
            ),
            (
                "runtime_discovery_side_effect_free".to_string(),
                "true".to_string(),
            ),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_capabilities(mut args: IntoIter<String>, format: OutputFormat) -> ExitCode {
    let scope = match CapabilityDiscoveryScope::parse(args.next().as_deref()) {
        Ok(scope) => scope,
        Err(error) => {
            return emit_error(
                "capabilities",
                format,
                "capability discovery failed",
                &error,
            );
        }
    };
    if let Some(extra) = args.next() {
        return emit_error(
            "capabilities",
            format,
            "capability discovery failed",
            &cli_unknown_arg_error("capabilities", &extra),
        );
    }
    if scope == CapabilityDiscoveryScope::Workflow {
        emit_workflow_capability_parity(scope, format);
        return ExitCode::SUCCESS;
    }
    if scope == CapabilityDiscoveryScope::Engines {
        emit_engine_mode_capabilities(scope, format);
        return ExitCode::SUCCESS;
    }
    if scope == CapabilityDiscoveryScope::RemoteApi {
        emit_remote_api_capability_parity(scope, format);
        return ExitCode::SUCCESS;
    }
    if scope == CapabilityDiscoveryScope::CrossCg {
        emit_cross_cg_capability_parity(scope, format);
        return ExitCode::SUCCESS;
    }
    if scope.world_class_dimension().is_some() {
        let report = plan_world_class_sufficiency();
        emit_world_class_surface_capability(scope, format, &report);
        return ExitCode::SUCCESS;
    }
    if scope != CapabilityDiscoveryScope::Engine {
        let report = CapabilityCertificationReport::contract_only();
        emit_capability_certification(scope, format, &report);
        return ExitCode::SUCCESS;
    }
    let capabilities = EngineCapabilities::current();
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "engine capabilities".to_string(),
        capabilities.to_human_text(),
        vec![],
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("native_input".to_string(), "vortex".to_string()),
            ("native_output".to_string(), "vortex".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
fn emit_engine_mode_capabilities(scope: CapabilityDiscoveryScope, format: OutputFormat) {
    let matrix = EngineCapabilityMatrixReport::cg22_contract();
    let mut fields =
        certification_common_fields(&CapabilityCertificationReport::contract_only(), scope);
    append_no_effect_parity_fields(&mut fields);
    push_field(
        &mut fields,
        "engine_capability_schema_version",
        matrix.schema_version,
    );
    push_field(&mut fields, "engine_capability_report_id", matrix.report_id);
    push_field(
        &mut fields,
        "engine_mode_vocabulary",
        &engine_mode_vocabulary(),
    );
    push_field(
        &mut fields,
        "boundedness_vocabulary",
        &boundedness_vocabulary(),
    );
    push_field(
        &mut fields,
        "update_mode_vocabulary",
        &update_mode_vocabulary(),
    );
    push_field(
        &mut fields,
        "output_mode_vocabulary",
        &output_mode_vocabulary(),
    );
    push_count_field(&mut fields, "engine_mode_count", matrix.rows.len());
    push_count_field(
        &mut fields,
        "partially_supported_engine_count",
        matrix.partially_supported_count(),
    );
    push_count_field(&mut fields, "planned_engine_count", matrix.planned_count());
    push_count_field(
        &mut fields,
        "live_hybrid_claim_blocked_count",
        matrix.live_hybrid_claim_blocked_count(),
    );
    push_field(&mut fields, "severity", "error");
    push_field(
        &mut fields,
        "blocker_ids",
        &engine_mode_blocker_ids(&matrix),
    );
    push_field(
        &mut fields,
        "required_evidence",
        engine_mode_required_evidence(),
    );
    push_field(
        &mut fields,
        "suggested_next_action",
        engine_mode_suggested_next_action(),
    );
    push_field(&mut fields, "future_rest_view", "/v1/capabilities/engines");
    for row in &matrix.rows {
        let prefix = row.engine_mode.as_str();
        push_field(
            &mut fields,
            &format!("{prefix}_support_status"),
            row.support_status.as_str(),
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_production_claim_allowed"),
            row.production_claim_allowed,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_state_required"),
            row.state_required,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_checkpoint_required"),
            row.checkpoint_required,
        );
        push_field(
            &mut fields,
            &format!("{prefix}_blocker_ids"),
            &engine_row_blocker_ids(row),
        );
        push_field(&mut fields, &format!("{prefix}_severity"), "error");
        push_field(
            &mut fields,
            &format!("{prefix}_required_evidence"),
            engine_row_required_evidence(row),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_suggested_next_action"),
            engine_mode_suggested_next_action(),
        );
        push_bool_field(&mut fields, &format!("{prefix}_no_runtime"), true);
        push_bool_field(&mut fields, &format!("{prefix}_no_fallback"), true);
        push_bool_field(&mut fields, &format!("{prefix}_no_effects"), true);
    }
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "engine mode capabilities".to_string(),
        matrix.to_human_text(),
        vec![],
        fields,
    );
}

fn emit_workflow_capability_parity(scope: CapabilityDiscoveryScope, format: OutputFormat) {
    let mut fields = parity_common_fields(
        scope,
        "shardloom.workflow_capability_parity.v1",
        "cg21.workflow_capability_parity",
        "cg21",
        "workflow_api,query_builder,dataframe_etl_affordances",
        "/v1/capabilities/workflow",
    );
    push_field(&mut fields, "workflow_state", "unsupported_report_only");
    push_count_field(&mut fields, "workflow_operation_count", 12);
    push_field(
        &mut fields,
        "workflow_operation_names",
        WORKFLOW_OPERATION_NAMES,
    );
    push_field(&mut fields, "severity", "error");
    push_field(&mut fields, "blocker_ids", WORKFLOW_BLOCKER_IDS);
    push_field(&mut fields, "required_evidence", WORKFLOW_REQUIRED_EVIDENCE);
    push_field(
        &mut fields,
        "suggested_next_action",
        WORKFLOW_SUGGESTED_NEXT_ACTION,
    );
    push_field(
        &mut fields,
        "unsupported_diagnostic_surface",
        "workflow-unsupported-plan",
    );
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "workflow capability parity".to_string(),
        parity_human_text(
            scope,
            "workflow unsupported diagnostics",
            WORKFLOW_BLOCKER_IDS,
        ),
        vec![],
        fields,
    );
}

fn emit_remote_api_capability_parity(scope: CapabilityDiscoveryScope, format: OutputFormat) {
    let mut fields = parity_common_fields(
        scope,
        "shardloom.remote_api_capability_parity.v1",
        "cg23.remote_api_capability_parity",
        "cg23",
        "rest_contract,plan_preview,lifecycle,event_stream,security_governance,data_plane",
        "/v1/capabilities/remote-api",
    );
    push_field(&mut fields, "remote_api_state", "contract_only_report_only");
    push_count_field(&mut fields, "remote_api_surface_count", 6);
    push_field(
        &mut fields,
        "remote_api_surface_names",
        "contract,plan_preview,local_lifecycle,event_stream,security_governance,data_plane",
    );
    push_field(&mut fields, "severity", "error");
    push_field(&mut fields, "blocker_ids", REMOTE_API_BLOCKER_IDS);
    push_field(
        &mut fields,
        "required_evidence",
        REMOTE_API_REQUIRED_EVIDENCE,
    );
    push_field(
        &mut fields,
        "suggested_next_action",
        REMOTE_API_SUGGESTED_NEXT_ACTION,
    );
    push_field(
        &mut fields,
        "unsupported_diagnostic_surface",
        "rest-api-plan-preview",
    );
    push_field(&mut fields, "contract_surface", "rest-api-contract-plan");
    push_field(&mut fields, "event_surface", "rest-api-event-stream");
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "remote api capability parity".to_string(),
        parity_human_text(scope, "remote api blockers", REMOTE_API_BLOCKER_IDS),
        vec![],
        fields,
    );
}

fn emit_cross_cg_capability_parity(scope: CapabilityDiscoveryScope, format: OutputFormat) {
    let matrix = EngineCapabilityMatrixReport::cg22_contract();
    let engine_blocker_ids = engine_mode_blocker_ids(&matrix);
    let blocker_ids =
        format!("{WORKFLOW_BLOCKER_IDS},{engine_blocker_ids},{REMOTE_API_BLOCKER_IDS}");
    let required_evidence = format!(
        "{WORKFLOW_REQUIRED_EVIDENCE},{},{}",
        engine_mode_required_evidence(),
        REMOTE_API_REQUIRED_EVIDENCE
    );
    let suggested_next_action = format!(
        "{} {} {}",
        WORKFLOW_SUGGESTED_NEXT_ACTION,
        engine_mode_suggested_next_action(),
        REMOTE_API_SUGGESTED_NEXT_ACTION
    );
    let mut fields = parity_common_fields(
        scope,
        "shardloom.cross_cg_capability_parity.v1",
        "cg21_cg22_cg23.cross_cg_capability_parity",
        "cg21,cg22,cg23",
        "workflow_api,engine_modes,remote_api",
        "/v1/capabilities/cross-cg",
    );
    push_count_field(&mut fields, "parity_surface_count", 3);
    push_field(&mut fields, "severity", "error");
    push_field(&mut fields, "blocker_ids", &blocker_ids);
    push_field(&mut fields, "required_evidence", &required_evidence);
    push_field(&mut fields, "suggested_next_action", &suggested_next_action);
    append_cross_cg_surface_fields(
        &mut fields,
        "cg21_workflow",
        "unsupported_report_only",
        WORKFLOW_BLOCKER_IDS,
        WORKFLOW_REQUIRED_EVIDENCE,
        WORKFLOW_SUGGESTED_NEXT_ACTION,
        "workflow-unsupported-plan",
    );
    append_cross_cg_surface_fields(
        &mut fields,
        "cg22_engine_modes",
        "partial_support_report_only",
        &engine_blocker_ids,
        engine_mode_required_evidence(),
        engine_mode_suggested_next_action(),
        "engine-capability-matrix",
    );
    append_cross_cg_surface_fields(
        &mut fields,
        "cg23_remote_api",
        "contract_only_report_only",
        REMOTE_API_BLOCKER_IDS,
        REMOTE_API_REQUIRED_EVIDENCE,
        REMOTE_API_SUGGESTED_NEXT_ACTION,
        "rest-api-plan-preview",
    );
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "cross-CG capability parity".to_string(),
        parity_human_text(
            scope,
            "workflow, engine, and remote api parity",
            WORKFLOW_BLOCKER_IDS,
        ),
        vec![],
        fields,
    );
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}

fn append_no_effect_parity_fields(fields: &mut Vec<(String, String)>) {
    push_bool_field(fields, "external_effects_executed", false);
    push_bool_field(fields, "data_read", false);
    push_bool_field(fields, "write_io", false);
    push_bool_field(fields, "no_runtime", true);
    push_bool_field(fields, "no_fallback", true);
    push_bool_field(fields, "no_effects", true);
}

fn parity_common_fields(
    scope: CapabilityDiscoveryScope,
    schema_version: &str,
    report_id: &str,
    represented_gates: &str,
    represented_surfaces: &str,
    future_rest_view: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![
        ("scope".to_string(), scope.as_str().to_string()),
        ("schema_version".to_string(), schema_version.to_string()),
        ("report_id".to_string(), report_id.to_string()),
        ("capability_status".to_string(), "report_only".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("side_effect_free".to_string(), "true".to_string()),
        ("filesystem_probe".to_string(), "false".to_string()),
        ("network_probe".to_string(), "false".to_string()),
        ("catalog_probe".to_string(), "false".to_string()),
        ("adapter_probe".to_string(), "false".to_string()),
        ("parser_executed".to_string(), "false".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
    ];
    append_no_effect_parity_fields(&mut fields);
    push_field(&mut fields, "represented_gates", represented_gates);
    push_field(&mut fields, "represented_surfaces", represented_surfaces);
    push_field(&mut fields, "future_rest_view", future_rest_view);
    fields
}

#[allow(clippy::too_many_arguments)]
fn append_cross_cg_surface_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    state: &str,
    blocker_ids: &str,
    required_evidence: &str,
    suggested_next_action: &str,
    diagnostic_surface: &str,
) {
    push_field(fields, &format!("{prefix}_state"), state);
    push_field(fields, &format!("{prefix}_severity"), "error");
    push_field(fields, &format!("{prefix}_blocker_ids"), blocker_ids);
    push_field(
        fields,
        &format!("{prefix}_required_evidence"),
        required_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_suggested_next_action"),
        suggested_next_action,
    );
    push_field(
        fields,
        &format!("{prefix}_diagnostic_surface"),
        diagnostic_surface,
    );
    push_bool_field(fields, &format!("{prefix}_no_runtime"), true);
    push_bool_field(fields, &format!("{prefix}_no_fallback"), true);
    push_bool_field(fields, &format!("{prefix}_no_effects"), true);
}

fn parity_human_text(scope: CapabilityDiscoveryScope, summary: &str, blocker_ids: &str) -> String {
    format!(
        "capability discovery: {}\nsummary: {}\nblocker_ids: {}\nfallback execution: disabled\nruntime execution: false\nside effects: none",
        scope.as_str(),
        summary,
        blocker_ids
    )
}

fn engine_row_blocker_ids(row: &shardloom_core::EngineCapabilityRow) -> String {
    row.blockers
        .iter()
        .map(|blocker| format!("cg22.engine.{}.{}", row.engine_mode.as_str(), blocker))
        .collect::<Vec<_>>()
        .join(",")
}

fn engine_mode_blocker_ids(matrix: &EngineCapabilityMatrixReport) -> String {
    matrix
        .rows
        .iter()
        .map(engine_row_blocker_ids)
        .collect::<Vec<_>>()
        .join(",")
}

fn engine_row_required_evidence(row: &shardloom_core::EngineCapabilityRow) -> &'static str {
    match row.engine_mode.as_str() {
        "batch" => {
            "workload_correctness_evidence,benchmark_evidence,broad_source_sink_certification"
        }
        "live" => {
            "durable_checkpoint_store,unbounded_runtime_scheduler,workload_correctness_evidence,benchmark_evidence"
        }
        "hybrid" => {
            "durable_micro_segment_flush_writes,object_store_commit_protocol,external_catalog_snapshot_discovery,workload_correctness_evidence,benchmark_evidence"
        }
        _ => engine_mode_required_evidence(),
    }
}

const fn engine_mode_required_evidence() -> &'static str {
    "workload_correctness_evidence,benchmark_evidence,broad_source_sink_certification,durable_checkpoint_store,object_store_commit_protocol"
}

const fn engine_mode_suggested_next_action() -> &'static str {
    "Use engine-selection-plan and engine-capability-matrix before making engine-mode execution claims."
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapabilityDiscoveryScope {
    Engine,
    Sql,
    Functions,
    Operators,
    Adapters,
    SemanticProfiles,
    Migration,
    Certification,
    DataEtl,
    Python,
    DataFrame,
    Notebook,
    Udfs,
    UniversalAdapters,
    EventApiSaasAdapters,
    UnstructuredMedia,
    ApiSurfaces,
    Observability,
    Deployment,
    Extensions,
    SecurityGovernance,
    Engines,
    Workflow,
    RemoteApi,
    CrossCg,
}

impl CapabilityDiscoveryScope {
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, ShardLoomError> {
        match value {
            None => Ok(Self::Engine),
            Some("sql") => Ok(Self::Sql),
            Some("functions") => Ok(Self::Functions),
            Some("operators") => Ok(Self::Operators),
            Some("adapters") => Ok(Self::Adapters),
            Some("semantic-profiles") => Ok(Self::SemanticProfiles),
            Some("migration") => Ok(Self::Migration),
            Some("certification") => Ok(Self::Certification),
            Some("data-etl") => Ok(Self::DataEtl),
            Some("python") => Ok(Self::Python),
            Some("dataframe") => Ok(Self::DataFrame),
            Some("notebook") => Ok(Self::Notebook),
            Some("udfs") => Ok(Self::Udfs),
            Some("universal-adapters") => Ok(Self::UniversalAdapters),
            Some("event-api-saas-adapters") => Ok(Self::EventApiSaasAdapters),
            Some("unstructured-media") => Ok(Self::UnstructuredMedia),
            Some("api-surfaces") => Ok(Self::ApiSurfaces),
            Some("observability") => Ok(Self::Observability),
            Some("deployment") => Ok(Self::Deployment),
            Some("extensions") => Ok(Self::Extensions),
            Some("security-governance") => Ok(Self::SecurityGovernance),
            Some("engines" | "engine-modes" | "engine_modes") => Ok(Self::Engines),
            Some("workflow" | "workflows" | "cg21-workflow" | "cg21_workflow") => {
                Ok(Self::Workflow)
            }
            Some("remote-api" | "remote_api" | "api-remote" | "cg23-remote-api") => {
                Ok(Self::RemoteApi)
            }
            Some("cross-cg" | "cross_cg" | "integrated" | "integrated-certification") => {
                Ok(Self::CrossCg)
            }
            Some(value) => Err(cli_unknown_arg_error("capabilities", value)),
        }
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Sql => "sql",
            Self::Functions => "functions",
            Self::Operators => "operators",
            Self::Adapters => "adapters",
            Self::SemanticProfiles => "semantic_profiles",
            Self::Migration => "migration",
            Self::Certification => "certification",
            Self::DataEtl => "data_etl",
            Self::Python => "python",
            Self::DataFrame => "dataframe",
            Self::Notebook => "notebook",
            Self::Udfs => "udfs",
            Self::UniversalAdapters => "universal_adapters",
            Self::EventApiSaasAdapters => "event_api_saas_adapters",
            Self::UnstructuredMedia => "unstructured_media",
            Self::ApiSurfaces => "api_surfaces",
            Self::Observability => "observability",
            Self::Deployment => "deployment",
            Self::Extensions => "extensions",
            Self::SecurityGovernance => "security_governance",
            Self::Engines => "engines",
            Self::Workflow => "workflow",
            Self::RemoteApi => "remote_api",
            Self::CrossCg => "cross_cg",
        }
    }

    #[must_use]
    pub(crate) const fn world_class_dimension(self) -> Option<WorldClassSufficiencyDimensionKind> {
        match self {
            Self::DataEtl => Some(WorldClassSufficiencyDimensionKind::DataEtlSurface),
            Self::Python => Some(WorldClassSufficiencyDimensionKind::PythonSurface),
            Self::DataFrame => Some(WorldClassSufficiencyDimensionKind::DataFrameQueryBuilder),
            Self::Notebook => Some(WorldClassSufficiencyDimensionKind::NotebookExperience),
            Self::Udfs => Some(WorldClassSufficiencyDimensionKind::UdfPlugin),
            Self::UniversalAdapters => {
                Some(WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog)
            }
            Self::EventApiSaasAdapters => {
                Some(WorldClassSufficiencyDimensionKind::EventApiSaasAdapters)
            }
            Self::UnstructuredMedia => Some(WorldClassSufficiencyDimensionKind::UnstructuredMedia),
            Self::ApiSurfaces => Some(WorldClassSufficiencyDimensionKind::ApiSurface),
            Self::Observability => Some(WorldClassSufficiencyDimensionKind::ObservabilitySurface),
            Self::Deployment => Some(WorldClassSufficiencyDimensionKind::DeploymentSurface),
            Self::Extensions => Some(WorldClassSufficiencyDimensionKind::ExtensionSurface),
            Self::SecurityGovernance => {
                Some(WorldClassSufficiencyDimensionKind::SecurityGovernance)
            }
            _ => None,
        }
    }
}

fn count_certification_status<I>(statuses: I, status: CapabilityCertificationStatus) -> usize
where
    I: Iterator<Item = CapabilityCertificationStatus>,
{
    statuses
        .filter(|entry_status| *entry_status == status)
        .count()
}

fn certification_common_fields(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> Vec<(String, String)> {
    vec![
        ("scope".to_string(), scope.as_str().to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted().to_string(),
        ),
        ("side_effect_free".to_string(), "true".to_string()),
        ("filesystem_probe".to_string(), "false".to_string()),
        ("network_probe".to_string(), "false".to_string()),
        ("catalog_probe".to_string(), "false".to_string()),
        ("adapter_probe".to_string(), "false".to_string()),
        ("parser_executed".to_string(), "false".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
    ]
}

pub(crate) fn certification_fields(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> Vec<(String, String)> {
    let mut fields = certification_common_fields(report, scope);
    match scope {
        CapabilityDiscoveryScope::Engine
        | CapabilityDiscoveryScope::DataEtl
        | CapabilityDiscoveryScope::Python
        | CapabilityDiscoveryScope::DataFrame
        | CapabilityDiscoveryScope::Notebook
        | CapabilityDiscoveryScope::Udfs
        | CapabilityDiscoveryScope::UniversalAdapters
        | CapabilityDiscoveryScope::EventApiSaasAdapters
        | CapabilityDiscoveryScope::UnstructuredMedia
        | CapabilityDiscoveryScope::ApiSurfaces
        | CapabilityDiscoveryScope::Observability
        | CapabilityDiscoveryScope::Deployment
        | CapabilityDiscoveryScope::Extensions
        | CapabilityDiscoveryScope::SecurityGovernance
        | CapabilityDiscoveryScope::Engines
        | CapabilityDiscoveryScope::Workflow
        | CapabilityDiscoveryScope::RemoteApi
        | CapabilityDiscoveryScope::CrossCg => {}
        CapabilityDiscoveryScope::Sql => append_sql_certification_fields(report, &mut fields),
        CapabilityDiscoveryScope::Functions => {
            append_function_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Operators => {
            append_operator_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Adapters => {
            append_adapter_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::SemanticProfiles => {
            append_semantic_profile_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Migration => {
            append_migration_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Certification => {
            append_full_certification_fields(report, &mut fields);
        }
    }
    fields
}

fn append_sql_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "sql_feature_count",
        report.sql_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "planned_count",
        count_certification_status(
            report.sql_coverage.entries.iter().map(|entry| entry.status),
            CapabilityCertificationStatus::Planned,
        ),
    );
    push_count_field(
        fields,
        "certified_count",
        count_certification_status(
            report.sql_coverage.entries.iter().map(|entry| entry.status),
            CapabilityCertificationStatus::Certified,
        ),
    );
}

fn append_function_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "function_group_count",
        report.function_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "planned_count",
        count_certification_status(
            report
                .function_coverage
                .entries
                .iter()
                .map(|entry| entry.status),
            CapabilityCertificationStatus::Planned,
        ),
    );
}

fn append_operator_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    let physical_plan = PhysicalOperatorPlan::cg7_foundation();
    let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
    push_count_field(
        fields,
        "operator_family_count",
        report.operator_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "production_certified_count",
        report
            .operator_coverage
            .entries
            .iter()
            .filter(|entry| entry.status.can_satisfy_production_claim())
            .count(),
    );
    push_field(
        fields,
        "physical_operator_schema_version",
        physical_plan.schema_version,
    );
    push_field(fields, "physical_operator_plan_id", &physical_plan.plan_id);
    push_count_field(
        fields,
        "physical_operator_count",
        physical_plan.operators.len(),
    );
    push_count_field(
        fields,
        "physical_operator_ready_count",
        physical_plan.ready_for_native_planning_count(),
    );
    push_count_field(
        fields,
        "physical_operator_missing_kernel_count",
        physical_plan.missing_kernel_count(),
    );
    push_count_field(
        fields,
        "physical_operator_unsupported_count",
        physical_plan.unsupported_count(),
    );
    push_field(
        fields,
        "physical_operator_fallback_execution_allowed",
        if physical_plan.fallback_execution_allowed() {
            "true"
        } else {
            "false"
        },
    );
    push_field(fields, "physical_operator_runtime_execution", "false");
    push_field(
        fields,
        "physical_operator_execution_profile_schema_version",
        execution_profiles.schema_version,
    );
    push_count_field(
        fields,
        "physical_operator_execution_profile_count",
        execution_profiles.profile_count(),
    );
    append_physical_operator_execution_level_fields(fields, &execution_profiles);
    push_count_field(
        fields,
        "physical_operator_reference_only_level_count",
        execution_profiles.reference_only_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_row_materialization_level_count",
        execution_profiles.row_materialization_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_arrow_conversion_level_count",
        execution_profiles.arrow_conversion_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_fallback_level_count",
        execution_profiles.fallback_allowed_count(),
    );
    append_metadata_physical_kernel_discovery_fields(fields);
    append_metadata_count_kernel_admission_discovery_fields(fields);
    append_metadata_filter_kernel_admission_discovery_fields(fields);
    append_metadata_projection_kernel_admission_discovery_fields(fields);
    append_encoded_projection_kernel_admission_discovery_fields(fields);
    append_encoded_count_physical_kernel_discovery_fields(fields);
    append_encoded_count_kernel_admission_discovery_fields(fields);
    append_encoded_predicate_evaluation_discovery_fields(fields);
    append_selection_vector_filter_kernel_discovery_fields(fields);
    append_selection_vector_filter_kernel_admission_discovery_fields(fields);
    append_encoded_count_local_guard_discovery_fields(fields);
    append_local_vortex_primitive_execution_discovery_fields(fields);
}

fn append_physical_operator_execution_level_fields(
    fields: &mut Vec<(String, String)>,
    execution_profiles: &PhysicalOperatorExecutionProfileMatrix,
) {
    push_count_field(
        fields,
        "physical_operator_native_execution_level_count",
        execution_profiles.native_execution_level_count(),
    );
    push_count_field(
        fields,
        "physical_operator_metadata_only_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::MetadataOnly),
    );
    push_count_field(
        fields,
        "physical_operator_encoded_native_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::EncodedNative),
    );
    push_count_field(
        fields,
        "physical_operator_hybrid_native_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::HybridNative),
    );
    push_count_field(
        fields,
        "physical_operator_native_decoded_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::NativeDecoded),
    );
}

fn append_metadata_physical_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_physical_kernel_schema_version",
        "shardloom.vortex_metadata_physical_kernel.v1",
    );
    push_field(
        fields,
        "metadata_physical_kernel_supported_primitives",
        "count_all,count_where,filter_predicate",
    );
    push_field(fields, "metadata_physical_kernel_contextual_only", "true");
    push_field(
        fields,
        "metadata_physical_kernel_requires_correctness_evidence",
        "true",
    );
    push_field(
        fields,
        "metadata_physical_kernel_requires_memory_safety_evidence",
        "true",
    );
    push_field(
        fields,
        "metadata_physical_kernel_requires_benchmark_for_production",
        "true",
    );
    push_field(fields, "metadata_physical_kernel_data_read", "false");
    push_field(fields, "metadata_physical_kernel_data_decoded", "false");
    push_field(
        fields,
        "metadata_physical_kernel_data_materialized",
        "false",
    );
    push_field(fields, "metadata_physical_kernel_object_store_io", "false");
    push_field(fields, "metadata_physical_kernel_write_io", "false");
    push_field(fields, "metadata_physical_kernel_spill_io", "false");
    push_field(
        fields,
        "metadata_physical_kernel_runtime_execution",
        "false",
    );
    push_field(
        fields,
        "metadata_physical_kernel_fallback_execution_allowed",
        "false",
    );
}

fn append_metadata_count_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_count_kernel_admission_schema_version",
        "shardloom.vortex_metadata_count_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_operator_kind",
        "count_aggregate",
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_metadata_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_metadata_filter_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_filter_kernel_admission_schema_version",
        "shardloom.vortex_metadata_filter_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_operator_kind",
        "filter",
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_metadata_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_metadata_projection_kernel_admission_discovery_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_field(
        fields,
        "metadata_projection_kernel_admission_schema_version",
        "shardloom.vortex_metadata_projection_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_projection_kernel_admission_operator_kind",
        "project",
    );
    push_field(
        fields,
        "metadata_projection_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_projection_readiness",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_projection_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "encoded_projection_kernel_admission_schema_version",
        "shardloom.vortex_encoded_projection_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "encoded_projection_kernel_admission_operator_kind",
        "project",
    );
    push_field(
        fields,
        "encoded_projection_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_projection_readiness",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_encoded_column_path",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_count_physical_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_count_physical_kernel_discovery_report();
    push_field(
        fields,
        "encoded_count_physical_kernel_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_id",
        report.kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_supported_primitive",
        report.supported_primitive.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_execution_certificate",
        report.requires_execution_certificate,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_correctness_evidence",
        report.requires_correctness_evidence,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_memory_safety_evidence",
        report.requires_memory_safety_evidence,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_benchmark_for_production",
        report.requires_benchmark_for_production,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_evaluated_path_reads_data",
        report.evaluated_path_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_encoded_count_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "encoded_count_kernel_admission_schema_version",
        "shardloom.vortex_encoded_count_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_operator_kind",
        "count_aggregate",
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_physical_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_predicate_evaluation_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_predicate_evaluation_discovery_report();
    push_field(
        fields,
        "encoded_predicate_evaluation_schema_version",
        report.schema_version,
    );
    push_field(fields, "encoded_predicate_evaluation_id", report.report_id);
    push_field(
        fields,
        "encoded_predicate_evaluation_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_predicate_evaluation_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_predicate_evaluation_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_emits_selection_vectors",
        report.emits_selection_vectors,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_supports_metadata_proven_all",
        report.supports_metadata_proven_all,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_supports_metadata_proven_none",
        report.supports_metadata_proven_none,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_defers_inconclusive_to_encoded_values",
        report.defers_inconclusive_predicates_to_encoded_values,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_selection_vector_filter_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_selection_vector_filter_kernel_discovery_report();
    push_field(
        fields,
        "selection_vector_filter_kernel_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_id",
        report.kernel_report_id,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_encoded_predicate_evaluation",
        report.requires_encoded_predicate_evaluation,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_selection_vectors",
        report.requires_selection_vectors,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_correctness_evidence",
        report.requires_correctness_evidence,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_memory_safety_evidence",
        report.requires_memory_safety_evidence,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_benchmark_for_production",
        report.requires_benchmark_for_production,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_selection_vector_filter_kernel_admission_discovery_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_schema_version",
        "shardloom.vortex_selection_vector_filter_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_operator_kind",
        "filter",
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_filter_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_count_local_guard_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_count_local_guard_discovery_report();
    push_field(
        fields,
        "encoded_count_local_guard_schema_version",
        report.schema_version,
    );
    push_field(fields, "encoded_count_local_guard_id", report.guard_id);
    push_field(
        fields,
        "encoded_count_local_guard_accepted_approval_sources",
        &report.accepted_approval_sources_text(),
    );
    push_field(
        fields,
        "encoded_count_local_guard_local_execution_status",
        report.local_execution_status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_local_guard_mode",
        report.mode.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_layout_row_count_path_accepted",
        report.layout_row_count_path_accepted,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_approved_local_scan_result_bridge_available",
        report.approved_local_scan_result_bridge_available,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report",
        report.approved_local_scan_result_bridge_requires_executed_report,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_returns_count_result",
        report.returns_count_result,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_side_effect_free",
        report.is_side_effect_free(),
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_materialized",
        report.data_materialized,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_runtime_execution",
        report.tasks_executed,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_local_vortex_primitive_execution_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "local_vortex_primitive_execution_schema_version",
        "shardloom.vortex_local_primitive_execution.v1",
    );
    push_field(
        fields,
        "local_vortex_primitive_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_field(
        fields,
        "local_vortex_primitive_execution_supported_primitives",
        "count_all,count_where,filter_predicate,project_columns,filter_and_project",
    );
    push_bool_field(fields, "local_vortex_primitive_execution_local_only", true);
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_count_all_decode_required",
        false,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_filter_project_decode_boundary_reported",
        false,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_scan_filter_pushdown",
        true,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_scan_projection_pushdown",
        true,
    );
    push_bool_field(fields, "local_vortex_primitive_execution_row_read", false);
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_arrow_converted",
        false,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_object_store_io",
        false,
    );
    push_bool_field(fields, "local_vortex_primitive_execution_write_io", false);
    push_bool_field(fields, "local_vortex_primitive_execution_spill_io", false);
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "local_vortex_primitive_execution_fallback_execution_allowed",
        false,
    );
}

fn append_adapter_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "adapter_entry_count",
        report.adapter_certification.entries.len(),
    );
    push_count_field(
        fields,
        "read_supported_count",
        report
            .adapter_certification
            .entries
            .iter()
            .filter(|entry| entry.read_supported)
            .count(),
    );
}

fn append_semantic_profile_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "semantic_profile_count",
        report.semantic_profiles.len(),
    );
    push_count_field(
        fields,
        "dimensions_declared_count",
        report
            .semantic_profiles
            .iter()
            .filter(|entry| entry.dimensions_declared)
            .count(),
    );
}

fn append_migration_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "migration_report_count",
        report.migration_reports.len(),
    );
    push_count_field(
        fields,
        "supported_construct_count",
        report
            .migration_reports
            .iter()
            .map(|entry| entry.supported_constructs.len())
            .sum::<usize>(),
    );
}

fn append_full_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "sql_feature_count",
        report.sql_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "operator_family_count",
        report.operator_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "function_group_count",
        report.function_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "adapter_entry_count",
        report.adapter_certification.entries.len(),
    );
    push_field(
        fields,
        "best_choice_claim",
        if report.can_publish_best_choice_claim() {
            "certified"
        } else {
            "not_certified"
        },
    );
}

fn certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    match scope {
        CapabilityDiscoveryScope::Engine => unreachable!("engine scope uses EngineCapabilities"),
        CapabilityDiscoveryScope::Sql => sql_certification_text(report, scope),
        CapabilityDiscoveryScope::Functions => function_certification_text(report, scope),
        CapabilityDiscoveryScope::Operators => operator_certification_text(report, scope),
        CapabilityDiscoveryScope::Adapters => adapter_certification_text(report, scope),
        CapabilityDiscoveryScope::SemanticProfiles => {
            semantic_profile_certification_text(report, scope)
        }
        CapabilityDiscoveryScope::Migration => migration_certification_text(report, scope),
        CapabilityDiscoveryScope::Certification => report.to_human_text(),
        CapabilityDiscoveryScope::DataEtl
        | CapabilityDiscoveryScope::Python
        | CapabilityDiscoveryScope::DataFrame
        | CapabilityDiscoveryScope::Notebook
        | CapabilityDiscoveryScope::Udfs
        | CapabilityDiscoveryScope::UniversalAdapters
        | CapabilityDiscoveryScope::EventApiSaasAdapters
        | CapabilityDiscoveryScope::UnstructuredMedia
        | CapabilityDiscoveryScope::ApiSurfaces
        | CapabilityDiscoveryScope::Observability
        | CapabilityDiscoveryScope::Deployment
        | CapabilityDiscoveryScope::Extensions
        | CapabilityDiscoveryScope::SecurityGovernance => {
            unreachable!("world-class user-surface scopes use WorldClassSufficiencyReport")
        }
        CapabilityDiscoveryScope::Engines => {
            unreachable!("engine-mode scope uses EngineCapabilityMatrixReport")
        }
        CapabilityDiscoveryScope::Workflow
        | CapabilityDiscoveryScope::RemoteApi
        | CapabilityDiscoveryScope::CrossCg => {
            unreachable!("cross-CG parity scopes use dedicated parity reports")
        }
    }
}

fn sql_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nsql coverage entries:\n{}",
        certification_summary_header(report, scope),
        report
            .sql_coverage
            .entries
            .iter()
            .map(|entry| format!(
                "  - {} [{} / {}]",
                entry.feature.as_str(),
                entry.status.as_str(),
                entry.tier.as_str()
            ))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn function_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nfunction coverage groups:\n{}",
        certification_summary_header(report, scope),
        report
            .function_coverage
            .entries
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.group.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn operator_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    let physical_plan = PhysicalOperatorPlan::cg7_foundation();
    let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
    let encoded_count_local_guard = vortex_encoded_count_local_guard_discovery_report();
    format!(
        "{}\noperator coverage families:\n{}\n{}\n{}\n{}\nlocal Vortex primitive execution: feature-gated count/filter/project/filter-and-project surface; count_all avoids decode, filter/project report materialization boundaries; fallback disabled",
        certification_summary_header(report, scope),
        report
            .operator_coverage
            .entries
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.family.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        physical_plan.to_human_text(),
        execution_profiles.to_human_text(),
        encoded_count_local_guard.to_human_text()
    )
}

fn adapter_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nadapter certification entries:\n{}",
        certification_summary_header(report, scope),
        report
            .adapter_certification
            .entries
            .iter()
            .map(|entry| {
                format!(
                    "  - {} [{} / {}]",
                    entry.adapter_id,
                    entry.status.as_str(),
                    entry.maturity.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn semantic_profile_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nsemantic profiles:\n{}",
        certification_summary_header(report, scope),
        report
            .semantic_profiles
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.profile.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn migration_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nmigration reports:\n{}",
        certification_summary_header(report, scope),
        report
            .migration_reports
            .iter()
            .map(|entry| {
                format!(
                    "  - {} [{}]",
                    entry.report_kind.as_str(),
                    entry.status.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn certification_summary_header(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "capability discovery: {}\nschema_version: {}\nfallback execution: disabled\nfallback_attempted: {}\nside effects: none\nstatus: planned/report-only",
        scope.as_str(),
        report.schema_version,
        report.fallback_attempted()
    )
}

pub(crate) fn emit_capability_certification(
    scope: CapabilityDiscoveryScope,
    format: OutputFormat,
    report: &CapabilityCertificationReport,
) {
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        format!("capability discovery: {}", scope.as_str()),
        certification_text(report, scope),
        report.diagnostics.clone(),
        certification_fields(report, scope),
    );
}

fn world_class_surface_components(scope: CapabilityDiscoveryScope) -> &'static str {
    match scope {
        CapabilityDiscoveryScope::DataEtl => {
            "ingestion,schema_contracts,data_quality,cleaning,transformation,enrichment,incremental_state,writes_exports,lineage_observability,governance"
        }
        CapabilityDiscoveryScope::Python => {
            "thin_cli_json_wrapper,python_api,diagnostics,materialization_boundaries,python_udf_boundaries,package_metadata,wheel_sdist_build,fresh_environment_smoke,conda_wrapper_cli_split"
        }
        CapabilityDiscoveryScope::DataFrame => {
            "dataframe_query_builder,expressions,lazy_plans,explain,materialization_boundaries"
        }
        CapabilityDiscoveryScope::Notebook => {
            "notebook_helpers,rich_diagnostics,explain_estimate_profile,display_materialization_boundaries"
        }
        CapabilityDiscoveryScope::Udfs => {
            "sql_udf,rust_udf,wasm_udf,python_udf,external_service_udf,sandboxing,effects"
        }
        CapabilityDiscoveryScope::UniversalAdapters => {
            "tabular_files,lakehouse_tables,object_stores,catalogs,relational_warehouses,events_apis_saas,python_notebook,unstructured_media"
        }
        CapabilityDiscoveryScope::EventApiSaasAdapters => {
            "event_streams,rest_apis,saas_exports,webhooks,rate_limits,credentials,effect_boundaries"
        }
        CapabilityDiscoveryScope::UnstructuredMedia => {
            "document_refs,media_refs,text_extraction,chunk_manifests,provenance,redaction,effect_permissions"
        }
        CapabilityDiscoveryScope::ApiSurfaces => {
            "cli_json,rust_api,python_api,query_builder,http_grpc,flightsql_like,jdbc_odbc"
        }
        CapabilityDiscoveryScope::Observability => {
            "explain,estimate,profile,diagnostics,certificates,lineage,metrics"
        }
        CapabilityDiscoveryScope::Deployment => {
            "cli_local,conda_cli_package,conda_python_package,conda_metapackage,server,container,cloud_storage,catalog_config,release_packaging,optional_benchmark_extras"
        }
        CapabilityDiscoveryScope::Extensions => {
            "plugin_manifest,udf_registry,wasm_runtime,python_boundary,permissions,sandboxing"
        }
        CapabilityDiscoveryScope::SecurityGovernance => {
            "credential_boundaries,redaction,audit,tenant_isolation,policy,provenance"
        }
        _ => unreachable!("non-world-class capability scope has no user-surface components"),
    }
}

fn world_class_surface_fields(
    scope: CapabilityDiscoveryScope,
    report: &WorldClassSufficiencyReport,
) -> Vec<(String, String)> {
    let kind = scope
        .world_class_dimension()
        .expect("world-class surface scope has dimension");
    let dimension = report
        .dimensions
        .iter()
        .find(|dimension| dimension.kind == kind)
        .expect("world-class sufficiency report includes all dimensions");
    vec![
        ("scope".to_string(), scope.as_str().to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "filesystem_probe".to_string(),
            report.filesystem_probe.to_string(),
        ),
        (
            "network_probe".to_string(),
            report.network_probe.to_string(),
        ),
        (
            "catalog_probe".to_string(),
            report.catalog_probe.to_string(),
        ),
        (
            "adapter_probe".to_string(),
            report.adapter_probe.to_string(),
        ),
        (
            "parser_executed".to_string(),
            report.parser_executed.to_string(),
        ),
        (
            "runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        ("dimension".to_string(), dimension.kind.as_str().to_string()),
        (
            "dimension_status".to_string(),
            dimension.status.as_str().to_string(),
        ),
        ("required".to_string(), dimension.required.to_string()),
        (
            "correctness_evidence_required".to_string(),
            dimension.correctness_evidence_required.to_string(),
        ),
        (
            "semantic_conformance_required".to_string(),
            dimension.semantic_conformance_required.to_string(),
        ),
        (
            "benchmark_evidence_required".to_string(),
            dimension.benchmark_evidence_required.to_string(),
        ),
        (
            "adapter_certification_required".to_string(),
            dimension.adapter_certification_required.to_string(),
        ),
        (
            "native_io_certificate_required".to_string(),
            dimension.native_io_certificate_required.to_string(),
        ),
        (
            "execution_certificate_required".to_string(),
            dimension.execution_certificate_required.to_string(),
        ),
        (
            "capability_snapshot_required".to_string(),
            dimension.capability_snapshot_required.to_string(),
        ),
        (
            "surface_components".to_string(),
            world_class_surface_components(scope).to_string(),
        ),
        (
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "best_default_publication_allowed".to_string(),
            report.can_publish_best_default_claim().to_string(),
        ),
    ]
}

fn world_class_surface_text(
    scope: CapabilityDiscoveryScope,
    report: &WorldClassSufficiencyReport,
) -> String {
    let kind = scope
        .world_class_dimension()
        .expect("world-class surface scope has dimension");
    let dimension_status = report.status_for(kind).as_str();
    format!(
        "capability discovery: {}\nschema_version: {}\nfallback execution: disabled\nfallback_attempted: {}\nside effects: none\ndimension: {}\ndimension_status: {}\nsurface_components: {}\nstatus: planned/report-only",
        scope.as_str(),
        report.schema_version,
        report.fallback_attempted,
        kind.as_str(),
        dimension_status,
        world_class_surface_components(scope)
    )
}

pub(crate) fn emit_world_class_surface_capability(
    scope: CapabilityDiscoveryScope,
    format: OutputFormat,
    report: &WorldClassSufficiencyReport,
) {
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        format!("capability discovery: {}", scope.as_str()),
        world_class_surface_text(scope, report),
        report.diagnostics.clone(),
        world_class_surface_fields(scope, report),
    );
}
