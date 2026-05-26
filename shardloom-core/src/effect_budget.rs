//! Effect-budget planning contracts.
//!
//! This module is report-only. It does not resolve credentials, probe external
//! systems, execute UDFs/plugins, call models/APIs, read files, or perform IO.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools
)]

use crate::{Diagnostic, DiagnosticSeverity};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectBudgetScope {
    LocalFileRead,
    LocalFileWrite,
    ObjectStoreRead,
    ObjectStoreWrite,
    CatalogRead,
    CatalogWrite,
    ApiRead,
    ApiWrite,
    LlmCall,
    EmbeddingGeneration,
    VectorSearch,
    PythonUdf,
    WasmUdf,
    ExternalServiceUdf,
    PluginExecution,
    MediaExtraction,
    NetworkEgress,
}

impl EffectBudgetScope {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFileRead => "local_file_read",
            Self::LocalFileWrite => "local_file_write",
            Self::ObjectStoreRead => "object_store_read",
            Self::ObjectStoreWrite => "object_store_write",
            Self::CatalogRead => "catalog_read",
            Self::CatalogWrite => "catalog_write",
            Self::ApiRead => "api_read",
            Self::ApiWrite => "api_write",
            Self::LlmCall => "llm_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::PythonUdf => "python_udf",
            Self::WasmUdf => "wasm_udf",
            Self::ExternalServiceUdf => "external_service_udf",
            Self::PluginExecution => "plugin_execution",
            Self::MediaExtraction => "media_extraction",
            Self::NetworkEgress => "network_egress",
        }
    }

    #[must_use]
    pub const fn requires_credentials(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreRead
                | Self::ObjectStoreWrite
                | Self::CatalogRead
                | Self::CatalogWrite
                | Self::ApiRead
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::ExternalServiceUdf
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn requires_redaction(&self) -> bool {
        matches!(
            self,
            Self::ApiRead
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::PythonUdf
                | Self::ExternalServiceUdf
                | Self::PluginExecution
                | Self::MediaExtraction
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn can_egress_data(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreWrite
                | Self::CatalogWrite
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::ExternalServiceUdf
                | Self::PluginExecution
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn is_destructive_or_mutating(&self) -> bool {
        matches!(
            self,
            Self::LocalFileWrite
                | Self::ObjectStoreWrite
                | Self::CatalogWrite
                | Self::ApiWrite
                | Self::PluginExecution
        )
    }

    #[must_use]
    pub const fn is_external(&self) -> bool {
        !matches!(
            self,
            Self::LocalFileRead | Self::LocalFileWrite | Self::WasmUdf
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectBudgetStatus {
    DeniedByDefault,
    Planned,
    RequiresApproval,
    ApprovedForPlan,
    Exceeded,
    Unsupported,
}

impl EffectBudgetStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DeniedByDefault => "denied_by_default",
            Self::Planned => "planned",
            Self::RequiresApproval => "requires_approval",
            Self::ApprovedForPlan => "approved_for_plan",
            Self::Exceeded => "exceeded",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_approved(&self) -> bool {
        matches!(self, Self::ApprovedForPlan)
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Exceeded | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectBlockerRow {
    pub row_id: &'static str,
    pub family: &'static str,
    pub operation: &'static str,
    pub support_status: &'static str,
    pub permission_status: &'static str,
    pub effect_status: &'static str,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub credential_required: bool,
    pub network_required: bool,
    pub sandbox_required: bool,
    pub model_or_embedding_call: bool,
    pub data_egress_possible: bool,
    pub materialization_boundary_required: bool,
    pub runtime_execution: bool,
    pub effect_executed: bool,
    pub claim_boundary: &'static str,
}

impl ExternalEffectBlockerRow {
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    const fn blocked(
        row_id: &'static str,
        family: &'static str,
        operation: &'static str,
        blocker_id: &'static str,
        required_evidence: &'static str,
        credential_required: bool,
        network_required: bool,
        sandbox_required: bool,
        model_or_embedding_call: bool,
        data_egress_possible: bool,
        materialization_boundary_required: bool,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            family,
            operation,
            support_status: "blocked",
            permission_status: "policy_required",
            effect_status: "denied_by_default",
            blocker_id,
            diagnostic_code: "SL_BLOCKED_EXTERNAL_EFFECT",
            required_evidence,
            credential_required,
            network_required,
            sandbox_required,
            model_or_embedding_call,
            data_egress_possible,
            materialization_boundary_required,
            runtime_execution: false,
            effect_executed: false,
            claim_boundary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectBlockerMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub docs_ref: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<ExternalEffectBlockerRow>,
    pub runtime_execution: bool,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl ExternalEffectBlockerMatrix {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.external_effect_blocker_matrix.v1",
            matrix_id: "gar-0032-c.udf_external_effect_blockers",
            docs_ref: "docs/architecture/udf-external-effect-blocker-matrix.md",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                ExternalEffectBlockerRow::blocked(
                    "sql_udf",
                    "udf",
                    "SQL-defined UDF",
                    "gar-0032-c.sql_udf_runtime_blocked",
                    "sql_parser,binder,function_registry,determinism_policy,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    false,
                    true,
                    "SQL-defined UDFs remain blocked; no parser, binder, planner, function registry, runtime, or fallback execution is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "rust_udf",
                    "udf",
                    "Rust-native UDF",
                    "gar-0032-c.rust_udf_runtime_blocked",
                    "function_registry,abi_contract,sandbox_policy,determinism_policy,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    false,
                    true,
                    "Rust UDFs remain blocked until registry, ABI, sandbox, determinism, and evidence contracts exist.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "wasm_udf",
                    "udf",
                    "WASM UDF",
                    "gar-0032-c.wasm_udf_runtime_blocked",
                    "wasm_runtime_policy,sandbox_policy,fuel_budget,memory_budget,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    false,
                    true,
                    "WASM UDFs remain blocked; no WASM runtime, sandbox, fuel, memory, or execution claim is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "python_udf",
                    "udf",
                    "Python UDF",
                    "gar-0032-c.python_udf_runtime_blocked",
                    "python_boundary,materialization_policy,sandbox_policy,redaction_policy,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    true,
                    true,
                    "Python UDFs remain blocked; no Python function execution, materialization, data egress, or fallback path is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "external_service_udf",
                    "udf",
                    "External service UDF",
                    "gar-0032-c.external_service_udf_runtime_blocked",
                    "credential_policy,network_policy,request_budget,idempotency_policy,audit_trail,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    true,
                    false,
                    true,
                    true,
                    "External service UDFs remain blocked; no credentials, network call, request execution, or external fallback is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "api_call",
                    "external_effect",
                    "API call",
                    "gar-0032-c.api_call_runtime_blocked",
                    "credential_policy,network_policy,rate_limit_policy,redaction_policy,audit_trail,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    "API calls remain blocked; no network request, credential resolution, or SaaS/REST runtime claim is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "llm_call",
                    "external_effect",
                    "LLM call",
                    "gar-0032-c.llm_call_runtime_blocked",
                    "model_policy,credential_policy,network_policy,cost_budget,redaction_policy,audit_trail,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    false,
                    true,
                    true,
                    true,
                    "LLM calls remain blocked; no model invocation, prompt egress, credential use, or network call is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "embedding_generation",
                    "external_effect",
                    "Embedding generation",
                    "gar-0032-c.embedding_generation_runtime_blocked",
                    "model_policy,credential_policy,network_policy,vector_schema,redaction_policy,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    false,
                    true,
                    true,
                    true,
                    "Embedding generation remains blocked; no model call, vector generation, network effect, or claim-grade vector support is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "vector_search",
                    "external_effect",
                    "Vector search",
                    "gar-0032-c.vector_search_runtime_blocked",
                    "vector_index_contract,query_semantics,credential_policy,network_policy,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    "Vector search remains blocked; no index, model, remote service, or similarity-runtime claim is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "plugin_execution",
                    "extension",
                    "Plugin execution",
                    "gar-0032-c.plugin_execution_runtime_blocked",
                    "plugin_manifest,abi_contract,sandbox_policy,permission_policy,audit_trail,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    true,
                    true,
                    "Plugin execution remains blocked until manifest, ABI, permission, sandbox, and audit evidence exist.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "media_extraction",
                    "unstructured_media",
                    "Media extraction",
                    "gar-0032-c.media_extraction_runtime_blocked",
                    "media_parser_policy,dependency_policy,redaction_policy,materialization_policy,effect_budget_certificate,no_fallback_evidence",
                    false,
                    false,
                    true,
                    false,
                    true,
                    true,
                    "Media extraction remains blocked; no OCR, transcription, parser dependency, model call, or extraction runtime is added.",
                ),
                ExternalEffectBlockerRow::blocked(
                    "network_egress",
                    "external_effect",
                    "Network egress",
                    "gar-0032-c.network_egress_blocked",
                    "network_policy,credential_policy,redaction_policy,request_budget,audit_trail,effect_budget_certificate,no_fallback_evidence",
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    "Network egress remains denied by default; no runtime network effect is authorized by this matrix.",
                ),
            ],
            runtime_execution: false,
            credential_resolution_performed: false,
            network_probe_performed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.blocker_id).collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn all_effects_blocked(&self) -> bool {
        self.rows.iter().all(|row| {
            row.support_status == "blocked" && !row.runtime_execution && !row.effect_executed
        }) && !self.runtime_execution
            && !self.credential_resolution_performed
            && !self.network_probe_performed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectfulOperationAdmissionRow {
    pub row_id: &'static str,
    pub family: &'static str,
    pub operation: &'static str,
    pub support_status: &'static str,
    pub admission_scope: &'static str,
    pub permission_status: &'static str,
    pub effect_status: &'static str,
    pub blocker_id: &'static str,
    pub diagnostic_code: &'static str,
    pub required_evidence: &'static str,
    pub credential_required: bool,
    pub network_required: bool,
    pub sandbox_required: bool,
    pub local_filesystem_io_allowed: bool,
    pub runtime_fixture_available: bool,
    pub extension_code_executed: bool,
    pub dynamic_loading_performed: bool,
    pub external_effect_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_boundary: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EffectfulBlockedRequirements {
    credential_required: bool,
    network_required: bool,
    sandbox_required: bool,
    local_filesystem_io_allowed: bool,
}

impl EffectfulBlockedRequirements {
    const CREDENTIAL_NETWORK: Self = Self {
        credential_required: true,
        network_required: true,
        sandbox_required: false,
        local_filesystem_io_allowed: false,
    };

    const SANDBOX: Self = Self {
        credential_required: false,
        network_required: false,
        sandbox_required: true,
        local_filesystem_io_allowed: false,
    };
}

impl EffectfulOperationAdmissionRow {
    #[allow(clippy::too_many_arguments)]
    const fn admitted_local(
        row_id: &'static str,
        family: &'static str,
        operation: &'static str,
        admission_scope: &'static str,
        required_evidence: &'static str,
        local_filesystem_io_allowed: bool,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            family,
            operation,
            support_status: "fixture_smoke_supported",
            admission_scope,
            permission_status: "local_policy_enforced",
            effect_status: "local_effect_only",
            blocker_id: "none",
            diagnostic_code: "none",
            required_evidence,
            credential_required: false,
            network_required: false,
            sandbox_required: false,
            local_filesystem_io_allowed,
            runtime_fixture_available: true,
            extension_code_executed: false,
            dynamic_loading_performed: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }

    const fn metadata_only(
        row_id: &'static str,
        family: &'static str,
        operation: &'static str,
        required_evidence: &'static str,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            family,
            operation,
            support_status: "metadata_only_supported",
            admission_scope: "typed_manifest_or_registry_inspection",
            permission_status: "not_required",
            effect_status: "no_effect",
            blocker_id: "none",
            diagnostic_code: "none",
            required_evidence,
            credential_required: false,
            network_required: false,
            sandbox_required: false,
            local_filesystem_io_allowed: false,
            runtime_fixture_available: false,
            extension_code_executed: false,
            dynamic_loading_performed: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }

    #[allow(clippy::too_many_arguments)]
    const fn blocked(
        row_id: &'static str,
        family: &'static str,
        operation: &'static str,
        blocker_id: &'static str,
        required_evidence: &'static str,
        requirements: EffectfulBlockedRequirements,
        claim_boundary: &'static str,
    ) -> Self {
        Self {
            row_id,
            family,
            operation,
            support_status: "blocked",
            admission_scope: "blocked_by_default",
            permission_status: "policy_required",
            effect_status: "denied_by_default",
            blocker_id,
            diagnostic_code: "SL_BLOCKED_EFFECTFUL_OPERATION",
            required_evidence,
            credential_required: requirements.credential_required,
            network_required: requirements.network_required,
            sandbox_required: requirements.sandbox_required,
            local_filesystem_io_allowed: requirements.local_filesystem_io_allowed,
            runtime_fixture_available: false,
            extension_code_executed: false,
            dynamic_loading_performed: false,
            external_effect_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_boundary,
        }
    }

    #[must_use]
    pub fn is_admitted_local_fixture(&self) -> bool {
        matches!(self.support_status, "fixture_smoke_supported")
    }

    #[must_use]
    pub fn is_blocked_external_or_sandboxed(&self) -> bool {
        self.support_status == "blocked"
            && (self.credential_required || self.network_required || self.sandbox_required)
            && !self.runtime_fixture_available
            && !self.extension_code_executed
            && !self.dynamic_loading_performed
            && !self.external_effect_executed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectfulOperationAdmissionMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub docs_ref: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<EffectfulOperationAdmissionRow>,
    pub credential_resolution_performed: bool,
    pub network_probe_performed: bool,
    pub dynamic_loading_performed: bool,
    pub extension_code_executed: bool,
    pub external_effect_executed: bool,
    pub dependency_expansion_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl EffectfulOperationAdmissionMatrix {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.effectful_operation_admission_matrix.v1",
            matrix_id: "gar-runtime-impl-4r-5o.effectful_operation_admission",
            docs_ref: "docs/architecture/effectful-operation-admission-matrix.md",
            claim_gate_status: "fixture_smoke_only",
            rows: vec![
                EffectfulOperationAdmissionRow::admitted_local(
                    "local_sqlite_import_export",
                    "database_file_adapter",
                    "local SQLite table export plus roundtrip import fixture",
                    "local_sqlite_file_table_scan_to_jsonl_and_roundtrip_sqlite",
                    "sqlite_file_digest,table_schema,column_order,row_count,workspace_safe_export,roundtrip_sqlite_row_count,no_fallback_evidence",
                    true,
                    "A local SQLite file may be table-scanned for fixture import/export smoke evidence only; no arbitrary SQL pushdown, network database, warehouse, production connector, or performance claim is added.",
                ),
                EffectfulOperationAdmissionRow::metadata_only(
                    "typed_extension_manifest_inspection",
                    "extension_manifest",
                    "typed extension/adapter manifest inspection",
                    "extension_id,version,category,license,capability_rows,permission_rows,effect_rows,sandbox_policy,no_fallback_evidence",
                    "Typed extension metadata can be inspected without dynamic loading, code execution, dependency expansion, credential resolution, network probes, or runtime enablement.",
                ),
                EffectfulOperationAdmissionRow::admitted_local(
                    "deterministic_scalar_udf_fixture",
                    "udf",
                    "built-in deterministic scalar UDF fixture",
                    "builtin_int64_null_propagating_scalar_fixture",
                    "udf_id,udf_version,input_dtype,output_dtype,determinism,null_policy,input_digest,output_digest,no_fallback_evidence",
                    false,
                    "Only the built-in deterministic scalar fixture is admitted; arbitrary Rust, WASM, Python, SQL-defined, table-function, and external-service UDFs remain blocked.",
                ),
                EffectfulOperationAdmissionRow::blocked(
                    "network_database_connectors",
                    "database_service",
                    "Postgres/MySQL/JDBC/ODBC/Snowflake/BigQuery/Databricks connectors",
                    "gar-runtime-impl-4r.database_service_network_connector_blocked",
                    "credential_policy,network_policy,connector_manifest,query_pushdown_contract,source_sink_certificates,audit_trail,no_fallback_evidence",
                    EffectfulBlockedRequirements::CREDENTIAL_NETWORK,
                    "Networked database and warehouse connectors remain blocked; no credentials, sockets, query pushdown, external execution, or fallback path is enabled.",
                ),
                EffectfulOperationAdmissionRow::blocked(
                    "rest_flight_adbc_connectors",
                    "external_adapter",
                    "REST/Flight/ADBC connector execution",
                    "gar-runtime-impl-4r.rest_flight_adbc_connector_blocked",
                    "protocol_contract,credential_policy,network_policy,request_budget,redaction_policy,audit_trail,no_fallback_evidence",
                    EffectfulBlockedRequirements::CREDENTIAL_NETWORK,
                    "REST, Flight, and ADBC adapter execution remains blocked; discovery metadata does not authorize network IO or external-engine use.",
                ),
                EffectfulOperationAdmissionRow::blocked(
                    "python_udf",
                    "udf",
                    "Python UDF execution",
                    "gar-runtime-impl-4r.python_udf_sandbox_blocked",
                    "python_boundary,sandbox_policy,materialization_policy,redaction_policy,execution_certificate,effect_budget,no_fallback_evidence",
                    EffectfulBlockedRequirements::SANDBOX,
                    "Python UDF execution remains blocked; no interpreter bridge, Python callable execution, materialization egress, or fallback path is added.",
                ),
                EffectfulOperationAdmissionRow::blocked(
                    "wasm_or_dynamic_plugin_udf",
                    "extension_udf",
                    "WASM/Rust dynamic plugin or UDF execution",
                    "gar-runtime-impl-4r.dynamic_plugin_udf_blocked",
                    "abi_contract,signature,provenance,sandbox_policy,fuel_budget,permission_policy,execution_certificate,no_fallback_evidence",
                    EffectfulBlockedRequirements::SANDBOX,
                    "WASM, Rust-native, and dynamic plugin UDF execution remain blocked; no dynamic loading or extension code execution is performed.",
                ),
                EffectfulOperationAdmissionRow::blocked(
                    "llm_api_embedding_vector_effects",
                    "external_effect",
                    "LLM/API/embedding/vector external effects",
                    "gar-runtime-impl-4r.external_model_api_effect_blocked",
                    "model_policy,credential_policy,network_policy,cost_budget,redaction_policy,vector_schema,audit_trail,no_fallback_evidence",
                    EffectfulBlockedRequirements::CREDENTIAL_NETWORK,
                    "LLM, API, embedding, and vector effects remain blocked; no model call, prompt/data egress, credential resolution, network probe, or service invocation is enabled.",
                ),
            ],
            credential_resolution_performed: false,
            network_probe_performed: false,
            dynamic_loading_performed: false,
            extension_code_executed: false,
            external_effect_executed: false,
            dependency_expansion_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn admitted_local_fixture_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.is_admitted_local_fixture())
            .count()
    }

    #[must_use]
    pub fn metadata_only_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == "metadata_only_supported")
            .count()
    }

    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == "blocked")
            .count()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter_map(|row| (row.blocker_id != "none").then_some(row.blocker_id))
            .collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn all_external_and_sandboxed_paths_blocked(&self) -> bool {
        !self.credential_resolution_performed
            && !self.network_probe_performed
            && !self.dynamic_loading_performed
            && !self.extension_code_executed
            && !self.external_effect_executed
            && !self.dependency_expansion_allowed
            && !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .filter(|row| {
                    row.credential_required || row.network_required || row.sandbox_required
                })
                .all(EffectfulOperationAdmissionRow::is_blocked_external_or_sandboxed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBudgetEntry {
    pub scope: EffectBudgetScope,
    pub status: EffectBudgetStatus,
    pub requested_call_count: u32,
    pub approved_call_count: u32,
    pub requested_egress_bytes: Option<u64>,
    pub approved_egress_bytes: Option<u64>,
    pub estimated_cost_micros: Option<u64>,
    pub approved_cost_micros: Option<u64>,
    pub credentials_required: bool,
    pub redaction_required: bool,
    pub audit_required: bool,
    pub approval_required: bool,
    pub materialization_boundary_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl EffectBudgetEntry {
    #[must_use]
    pub fn denied_by_default(scope: EffectBudgetScope) -> Self {
        Self {
            scope,
            status: EffectBudgetStatus::DeniedByDefault,
            requested_call_count: 0,
            approved_call_count: 0,
            requested_egress_bytes: None,
            approved_egress_bytes: None,
            estimated_cost_micros: None,
            approved_cost_micros: None,
            credentials_required: scope.requires_credentials(),
            redaction_required: scope.requires_redaction(),
            audit_required: scope.is_external() || scope.is_destructive_or_mutating(),
            approval_required: scope.is_external() || scope.is_destructive_or_mutating(),
            materialization_boundary_required: scope.can_egress_data()
                || matches!(
                    scope,
                    EffectBudgetScope::PythonUdf
                        | EffectBudgetScope::ExternalServiceUdf
                        | EffectBudgetScope::MediaExtraction
                ),
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn effect_allowed(&self) -> bool {
        self.status.is_approved() && self.approved_call_count > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBudgetReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub budget_mode: &'static str,
    pub entries: Vec<EffectBudgetEntry>,
    pub external_effects_allowed: bool,
    pub destructive_effects_allowed: bool,
    pub network_egress_allowed: bool,
    pub credentials_resolved: bool,
    pub secrets_loaded: bool,
    pub redaction_policy_required: bool,
    pub audit_required: bool,
    pub runtime_execution_performed: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl EffectBudgetReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.effect_budget.v1",
            report_id: "cross_cutting.effect_budget",
            budget_mode: "deny_external_effects_by_default",
            entries: Self::default_scopes()
                .iter()
                .copied()
                .map(EffectBudgetEntry::denied_by_default)
                .collect(),
            external_effects_allowed: false,
            destructive_effects_allowed: false,
            network_egress_allowed: false,
            credentials_resolved: false,
            secrets_loaded: false,
            redaction_policy_required: true,
            audit_required: true,
            runtime_execution_performed: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub const fn default_scopes() -> &'static [EffectBudgetScope] {
        &[
            EffectBudgetScope::LocalFileRead,
            EffectBudgetScope::LocalFileWrite,
            EffectBudgetScope::ObjectStoreRead,
            EffectBudgetScope::ObjectStoreWrite,
            EffectBudgetScope::CatalogRead,
            EffectBudgetScope::CatalogWrite,
            EffectBudgetScope::ApiRead,
            EffectBudgetScope::ApiWrite,
            EffectBudgetScope::LlmCall,
            EffectBudgetScope::EmbeddingGeneration,
            EffectBudgetScope::VectorSearch,
            EffectBudgetScope::PythonUdf,
            EffectBudgetScope::WasmUdf,
            EffectBudgetScope::ExternalServiceUdf,
            EffectBudgetScope::PluginExecution,
            EffectBudgetScope::MediaExtraction,
            EffectBudgetScope::NetworkEgress,
        ]
    }

    #[must_use]
    pub fn denied_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == EffectBudgetStatus::DeniedByDefault)
            .count()
    }

    #[must_use]
    pub fn approved_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.effect_allowed())
            .count()
    }

    #[must_use]
    pub fn approval_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.approval_required)
            .count()
    }

    #[must_use]
    pub fn credential_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.credentials_required)
            .count()
    }

    #[must_use]
    pub fn materialization_boundary_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.materialization_boundary_required)
            .count()
    }

    #[must_use]
    pub fn scope_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.scope.as_str())
            .collect()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.runtime_execution_performed
            && !self.filesystem_probe
            && !self.network_probe
            && !self.catalog_probe
            && !self.external_effects_allowed
            && !self.destructive_effects_allowed
            && !self.network_egress_allowed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self.approved_scope_count() == 0
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.entries.iter().any(EffectBudgetEntry::has_errors)
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "budget_mode: {}", self.budget_mode);
        let _ = writeln!(
            out,
            "external effects allowed: {}",
            self.external_effects_allowed
        );
        let _ = writeln!(
            out,
            "destructive effects allowed: {}",
            self.destructive_effects_allowed
        );
        let _ = writeln!(
            out,
            "network egress allowed: {}",
            self.network_egress_allowed
        );
        let _ = writeln!(out, "credentials resolved: {}", self.credentials_resolved);
        let _ = writeln!(out, "secrets loaded: {}", self.secrets_loaded);
        let _ = writeln!(
            out,
            "runtime execution: {}",
            self.runtime_execution_performed
        );
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "effect scopes:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] approval_required={} credentials_required={} materialization_boundary_required={}",
                entry.scope.as_str(),
                entry.status.as_str(),
                entry.approval_required,
                entry.credentials_required,
                entry.materialization_boundary_required
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_default_denies_effects_without_runtime_work() {
        let report = EffectBudgetReport::planning_default();
        assert_eq!(report.schema_version, "shardloom.effect_budget.v1");
        assert_eq!(
            report.entries.len(),
            EffectBudgetReport::default_scopes().len()
        );
        assert_eq!(report.approved_scope_count(), 0);
        assert_eq!(report.denied_scope_count(), report.entries.len());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn default_report_tracks_credential_and_materialization_boundaries() {
        let report = EffectBudgetReport::planning_default();
        assert!(report.credential_required_scope_count() > 0);
        assert!(report.materialization_boundary_required_scope_count() > 0);
        assert!(
            report
                .scope_order()
                .contains(&EffectBudgetScope::LlmCall.as_str())
        );
        assert!(
            report
                .scope_order()
                .contains(&EffectBudgetScope::NetworkEgress.as_str())
        );
    }

    #[test]
    fn external_effect_blocker_matrix_denies_every_effect_by_default() {
        let matrix = ExternalEffectBlockerMatrix::report_only();
        assert_eq!(
            matrix.schema_version,
            "shardloom.external_effect_blocker_matrix.v1"
        );
        assert_eq!(matrix.claim_gate_status, "not_claim_grade");
        assert!(matrix.all_effects_blocked());
        assert!(!matrix.runtime_execution);
        assert!(!matrix.credential_resolution_performed);
        assert!(!matrix.network_probe_performed);
        assert!(!matrix.fallback_attempted);
        assert!(!matrix.external_engine_invoked);
        assert!(matrix.row_order().contains(&"python_udf"));
        assert!(matrix.row_order().contains(&"llm_call"));
        assert!(matrix.row_order().contains(&"embedding_generation"));
        assert!(matrix.row_order().contains(&"network_egress"));
        assert!(
            matrix
                .rows
                .iter()
                .all(|row| row.support_status == "blocked")
        );
        assert!(
            matrix
                .rows
                .iter()
                .all(|row| row.effect_status == "denied_by_default")
        );
        assert!(matrix.rows.iter().all(|row| !row.runtime_execution));
        assert!(matrix.rows.iter().all(|row| !row.effect_executed));
    }

    #[test]
    fn effectful_operation_admission_matrix_admits_only_local_fixtures() {
        let matrix = EffectfulOperationAdmissionMatrix::current();
        assert_eq!(
            matrix.schema_version,
            "shardloom.effectful_operation_admission_matrix.v1"
        );
        assert_eq!(matrix.claim_gate_status, "fixture_smoke_only");
        assert_eq!(matrix.admitted_local_fixture_count(), 2);
        assert_eq!(matrix.metadata_only_count(), 1);
        assert!(matrix.blocked_count() >= 4);
        assert!(matrix.row_order().contains(&"local_sqlite_import_export"));
        assert!(
            matrix
                .row_order()
                .contains(&"deterministic_scalar_udf_fixture")
        );
        assert!(
            matrix
                .row_order()
                .contains(&"typed_extension_manifest_inspection")
        );
        assert!(matrix.all_external_and_sandboxed_paths_blocked());
        assert!(!matrix.credential_resolution_performed);
        assert!(!matrix.network_probe_performed);
        assert!(!matrix.dynamic_loading_performed);
        assert!(!matrix.extension_code_executed);
        assert!(!matrix.external_effect_executed);
        assert!(!matrix.fallback_attempted);
        assert!(!matrix.external_engine_invoked);
    }

    #[test]
    fn probes_or_fallbacks_make_report_unsafe() {
        let mut report = EffectBudgetReport::planning_default();
        report.network_probe = true;
        assert!(!report.side_effect_free());
        assert!(report.has_errors());

        let mut fallback = EffectBudgetReport::planning_default();
        fallback.fallback_attempted = true;
        assert!(fallback.has_errors());
    }
}
