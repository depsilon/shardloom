use std::process::Command;

const REPORT_ONLY_BOOL_FIELD_KEYS: [&str; 9] = [
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
];

const PLANNER_READINESS_FIELD_KEYS: [&str; 21] = [
    "planner_readiness_schema_version",
    "planner_readiness_matrix_id",
    "planner_readiness_report_ref",
    "planner_readiness_docs_ref",
    "planner_readiness_support_status_vocabulary",
    "planner_readiness_claim_gate_status",
    "planner_readiness_row_count",
    "planner_readiness_row_order",
    "planner_readiness_sql_row_order",
    "planner_readiness_dataframe_row_order",
    "planner_readiness_unsupported_diagnostic_codes",
    "planner_readiness_blocker_ids",
    "planner_readiness_required_evidence",
    "planner_readiness_parser_executed",
    "planner_readiness_binder_executed",
    "planner_readiness_planner_executed",
    "planner_readiness_runtime_execution",
    "planner_readiness_dataframe_runtime",
    "planner_readiness_external_engine_invoked",
    "planner_readiness_fallback_attempted",
    "planner_readiness_deterministic_diagnostics_present",
];

const GENERATED_SOURCE_FIELD_KEYS: [&str; 63] = [
    "generated_source_contract_schema_version",
    "generated_source_contract_report_id",
    "generated_source_certificate_schema_version",
    "generated_source_support_status_vocabulary",
    "generated_source_case_count",
    "generated_source_case_order",
    "generated_source_required_field_order",
    "generated_source_contract_claim_gate_status",
    "generated_source_contract_fallback_attempted",
    "generated_source_contract_external_engine_invoked",
    "generated_source_contract_object_store_io_performed",
    "generated_source_contract_foundry_runtime_invoked",
    "generated_source_contract_broad_sql_dataframe_claim_allowed",
    "no_dataset_smoke_support_status",
    "no_dataset_smoke_generated_source_certificate_status",
    "no_dataset_smoke_input_dataset_count",
    "no_dataset_smoke_source_io_performed",
    "no_dataset_smoke_generated_source_created",
    "no_dataset_smoke_output_io_performed",
    "no_dataset_smoke_source_native_io_certificate_status",
    "no_dataset_smoke_output_native_io_certificate_status",
    "no_dataset_smoke_required_generator_kinds",
    "no_dataset_smoke_required_evidence_fields",
    "no_dataset_smoke_blocker_id",
    "no_dataset_smoke_claim_gate_status",
    "no_dataset_smoke_claim_boundary",
    "no_dataset_smoke_fallback_attempted",
    "no_dataset_smoke_external_engine_invoked",
    "user_generated_source_support_status",
    "user_generated_source_generated_source_certificate_status",
    "user_generated_source_input_dataset_count",
    "user_generated_source_source_io_performed",
    "user_generated_source_generated_source_created",
    "user_generated_source_output_io_performed",
    "user_generated_source_source_native_io_certificate_status",
    "user_generated_source_output_native_io_certificate_status",
    "user_generated_source_required_generator_kinds",
    "user_generated_source_required_evidence_fields",
    "user_generated_source_blocker_id",
    "user_generated_source_claim_gate_status",
    "user_generated_source_claim_boundary",
    "user_generated_source_fallback_attempted",
    "user_generated_source_external_engine_invoked",
    "engine_native_generated_source_support_status",
    "engine_native_generated_source_generated_source_certificate_status",
    "engine_native_generated_source_input_dataset_count",
    "engine_native_generated_source_source_io_performed",
    "engine_native_generated_source_generated_source_created",
    "engine_native_generated_source_output_io_performed",
    "engine_native_generated_source_source_native_io_certificate_status",
    "engine_native_generated_source_output_native_io_certificate_status",
    "engine_native_generated_source_required_generator_kinds",
    "engine_native_generated_source_required_evidence_fields",
    "engine_native_generated_source_blocker_id",
    "engine_native_generated_source_claim_gate_status",
    "engine_native_generated_source_claim_boundary",
    "engine_native_generated_source_fallback_attempted",
    "engine_native_generated_source_external_engine_invoked",
    "input_dataset_count",
    "source_io_performed",
    "generated_source_created",
    "output_io_performed",
    "generated_source_certificate_status",
];

const GENERATED_SOURCE_API_ADMISSION_FIELD_KEYS: [&str; 20] = [
    "generated_source_api_admission_schema_version",
    "generated_source_api_admission_matrix_id",
    "generated_source_api_admission_support_status_vocabulary",
    "generated_source_api_admission_claim_gate_status",
    "generated_source_api_admission_row_count",
    "generated_source_api_admission_row_order",
    "generated_source_api_admission_python_row_order",
    "generated_source_api_admission_sql_row_order",
    "generated_source_api_admission_dataframe_row_order",
    "generated_source_api_admission_blocker_ids",
    "generated_source_api_admission_required_evidence",
    "generated_source_api_admission_runtime_execution",
    "generated_source_api_admission_data_read",
    "generated_source_api_admission_write_io",
    "generated_source_api_admission_source_io_performed",
    "generated_source_api_admission_generated_source_created",
    "generated_source_api_admission_fallback_attempted",
    "generated_source_api_admission_external_engine_invoked",
    "generated_source_api_admission_fallback_execution_allowed",
    "generated_source_api_admission_broad_sql_dataframe_claim_allowed",
];

const GENERATED_SOURCE_API_ADMISSION_ROW_IDS: [&str; 12] = [
    "python_ctx_from_rows",
    "python_ctx_range",
    "python_ctx_sequence",
    "python_ctx_literal_table",
    "python_ctx_calendar",
    "python_generated_source_write",
    "sql_literal_select",
    "sql_values",
    "sql_source_free_projection",
    "sql_generate_series_range",
    "dataframe_source_free_projection",
    "dataframe_generated_with_column",
];

const GENERATED_SOURCE_API_ADMISSION_ROW_SUFFIXES: [&str; 12] = [
    "support_status",
    "runtime_execution",
    "data_read",
    "write_io",
    "source_io_performed",
    "generated_source_created",
    "blocker_id",
    "required_evidence",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
    "fallback_execution_allowed",
];

const GENERATED_SOURCE_EVIDENCE_ALIGNMENT_FIELD_KEYS: [&str; 20] = [
    "generated_source_evidence_alignment_schema_version",
    "generated_source_evidence_alignment_report_id",
    "generated_source_evidence_alignment_docs_ref",
    "generated_source_evidence_alignment_contract_ref",
    "generated_source_evidence_alignment_api_admission_ref",
    "generated_source_evidence_alignment_openlineage_ref",
    "generated_source_evidence_alignment_opentelemetry_ref",
    "generated_source_evidence_alignment_bayesian_confidence_ref",
    "generated_source_evidence_alignment_row_count",
    "generated_source_evidence_alignment_row_order",
    "generated_source_evidence_alignment_openlineage_export_enabled",
    "generated_source_evidence_alignment_opentelemetry_export_enabled",
    "generated_source_evidence_alignment_opentelemetry_network_exporter_enabled",
    "generated_source_evidence_alignment_bayesian_confidence_enabled",
    "generated_source_evidence_alignment_foundry_runtime_invoked",
    "generated_source_evidence_alignment_object_store_io_performed",
    "generated_source_evidence_alignment_fallback_attempted",
    "generated_source_evidence_alignment_external_engine_invoked",
    "generated_source_evidence_alignment_all_rows_no_fallback_no_external_engine",
    "generated_source_evidence_alignment_claim_gate_status",
];

const GENERATED_SOURCE_EVIDENCE_ALIGNMENT_ROW_IDS: [&str; 4] = [
    "no_dataset_smoke",
    "python_generated_source_write",
    "sql_dataframe_source_free",
    "foundry_generated_output",
];

const GENERATED_SOURCE_EVIDENCE_ALIGNMENT_ROW_SUFFIXES: [&str; 15] = [
    "surface",
    "source_free_case",
    "support_status",
    "runtime_execution",
    "generated_source_certificate_status",
    "output_native_io_certificate_status",
    "openlineage_facet_status",
    "opentelemetry_span_status",
    "bayesian_confidence_status",
    "foundry_boundary_ref",
    "blocker_id",
    "required_evidence",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
];

const WRAPPER_CONNECTOR_REGISTRY_FIELD_KEYS: [&str; 17] = [
    "wrapper_connector_registry_schema_version",
    "wrapper_connector_registry_report_id",
    "wrapper_connector_registry_docs_ref",
    "wrapper_connector_registry_support_status_vocabulary",
    "wrapper_connector_registry_row_count",
    "wrapper_connector_registry_row_order",
    "wrapper_connector_registry_ready_local_count",
    "wrapper_connector_registry_report_only_count",
    "wrapper_connector_registry_blocked_count",
    "wrapper_connector_registry_diagnostic_codes",
    "wrapper_connector_registry_required_evidence",
    "wrapper_connector_registry_dependency_expansion_allowed",
    "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
    "wrapper_connector_registry_fallback_attempted",
    "wrapper_connector_registry_external_engine_invoked",
    "wrapper_connector_registry_all_rows_no_fallback_no_external_engine",
    "wrapper_connector_registry_claim_gate_status",
];

const COMMAND_REGISTRY_FIELD_KEYS: [&str; 24] = [
    "command_registry_schema_version",
    "command_registry_report_id",
    "command_registry_docs_ref",
    "command_registry_source",
    "command_registry_metadata_command",
    "command_registry_help_command",
    "command_registry_registered_command_count",
    "command_registry_support_state_vocabulary",
    "command_registry_row_order",
    "command_registry_family_order",
    "command_registry_executable_count",
    "command_registry_feature_gated_count",
    "command_registry_diagnostic_only_count",
    "command_registry_report_only_count",
    "command_registry_blocked_count",
    "command_registry_future_count",
    "command_registry_evidence_fields",
    "command_registry_claim_boundary",
    "command_registry_fallback_boundary",
    "command_registry_fallback_attempted",
    "command_registry_external_engine_invoked",
    "command_registry_all_commands_have_usage_fragment",
    "command_registry_all_commands_classified",
    "command_registry_claim_gate_status",
];

const COMMAND_REGISTRY_ROW_SUFFIXES: [&str; 12] = [
    "command",
    "family",
    "support_state",
    "side_effect_level",
    "usage_fragment",
    "feature_gate_status",
    "input_contract",
    "output_contract",
    "evidence_fields",
    "owning_phase_item",
    "fallback_attempted",
    "external_engine_invoked",
];

const EVIDENCE_SCHEMA_REGISTRY_FIELD_KEYS: [&str; 18] = [
    "evidence_schema_registry_schema_version",
    "evidence_schema_registry_report_id",
    "evidence_schema_registry_source",
    "evidence_schema_registry_docs_ref",
    "evidence_schema_registry_command",
    "evidence_schema_registry_surface_count",
    "evidence_schema_registry_field_count",
    "evidence_schema_registry_surface_order",
    "evidence_schema_registry_dtype_vocabulary",
    "evidence_schema_registry_cardinality_vocabulary",
    "evidence_schema_registry_deprecation_policy",
    "evidence_schema_registry_claim_boundary",
    "evidence_schema_registry_fallback_boundary",
    "evidence_schema_registry_claim_gate_status",
    "evidence_schema_registry_schema_drift_detection_status",
    "evidence_schema_registry_python_accessor_contract_status",
    "evidence_schema_registry_fallback_attempted",
    "evidence_schema_registry_external_engine_invoked",
];

const EVIDENCE_SCHEMA_SURFACE_IDS: [&str; 8] = [
    "execution_mode_selection_report",
    "compute_flow_evidence",
    "execution_certificate_report",
    "native_io_report",
    "benchmark_plan_report",
    "benchmark_constitution_report",
    "benchmark_claim_evidence_report",
    "compute_capability_matrix_report",
];

const EVIDENCE_SCHEMA_SURFACE_SUFFIXES: [&str; 9] = [
    "artifact_kind",
    "command_examples",
    "support_state",
    "field_count",
    "field_order",
    "python_accessor_mapping",
    "required_no_fallback_fields",
    "claim_boundary",
    "deprecation_policy",
];

const EVIDENCE_SCHEMA_FIELD_SUFFIXES: [&str; 12] = [
    "key",
    "dtype",
    "cardinality",
    "required_when",
    "owning_surface",
    "owning_artifact_kind",
    "owning_commands",
    "support_state",
    "no_fallback_semantics",
    "deprecation_policy",
    "python_accessor_mapping",
    "claim_boundary",
];

const WRAPPER_CONNECTOR_REGISTRY_ROW_IDS: [&str; 26] = [
    "python_cli_json_client",
    "python_typed_capability_views",
    "python_generated_source_helpers",
    "rust_client",
    "typescript_javascript_client",
    "go_client",
    "java_jvm_client",
    "dotnet_client",
    "r_client",
    "rest_openapi_generated_client",
    "ci_report_viewer",
    "foundry_transform_wrapper",
    "python_dbapi",
    "sqlalchemy",
    "ibis",
    "dbt",
    "airflow",
    "dagster",
    "prefect",
    "mcp",
    "flight_sql",
    "adbc",
    "jdbc_via_flight_sql",
    "odbc",
    "bi_connector",
    "grafana_datasource",
];

const WRAPPER_CONNECTOR_REGISTRY_ROW_SUFFIXES: [&str; 17] = [
    "family",
    "planned_package",
    "maturity",
    "primary_transport",
    "support_status",
    "user_visible_surface",
    "implementation_evidence",
    "deterministic_diagnostic_code",
    "required_evidence",
    "explicit_execution_available",
    "dependency_added",
    "network_listener_started",
    "data_plane_bridge_supported",
    "external_engine_invoked",
    "fallback_attempted",
    "claim_gate_status",
    "claim_boundary",
];

const OPENLINEAGE_FACET_MAPPING_FIELD_KEYS: [&str; 23] = [
    "openlineage_facet_mapping_schema_version",
    "openlineage_facet_mapping_report_id",
    "openlineage_facet_mapping_gar_id",
    "openlineage_facet_mapping_docs_ref",
    "openlineage_facet_mapping_object_model_ref",
    "openlineage_facet_mapping_facets_ref",
    "openlineage_facet_mapping_custom_facets_ref",
    "openlineage_facet_mapping_producer_placeholder",
    "openlineage_facet_mapping_schema_url_base_placeholder",
    "openlineage_facet_mapping_row_count",
    "openlineage_facet_mapping_row_order",
    "openlineage_facet_mapping_export_enabled",
    "openlineage_facet_mapping_event_emitted",
    "openlineage_facet_mapping_network_call_performed",
    "openlineage_facet_mapping_backend_configured",
    "openlineage_facet_mapping_client_dependency_added",
    "openlineage_facet_mapping_schema_published",
    "openlineage_facet_mapping_redaction_policy_required",
    "openlineage_facet_mapping_retention_policy_required",
    "openlineage_facet_mapping_opt_in_required",
    "openlineage_facet_mapping_all_rows_report_only",
    "openlineage_facet_mapping_all_rows_no_fallback_no_external_engine",
    "openlineage_facet_mapping_claim_gate_status",
];

const OPENLINEAGE_FACET_MAPPING_ROW_IDS: [&str; 7] = [
    "execution_mode",
    "no_fallback",
    "native_io_certificate",
    "materialization_boundary",
    "claim_gate",
    "generated_source",
    "vortex_artifact",
];

const OPENLINEAGE_FACET_MAPPING_ROW_SUFFIXES: [&str; 17] = [
    "facet_name",
    "facet_key",
    "openlineage_entity",
    "shardloom_evidence_fields",
    "schema_url_placeholder",
    "schema_version",
    "producer",
    "facet_status",
    "export_enabled",
    "event_emitted",
    "network_call_performed",
    "redaction_required",
    "retention_policy_required",
    "claim_gate_status",
    "claim_boundary",
    "fallback_attempted",
    "external_engine_invoked",
];

const OPENTELEMETRY_TRACE_EXPORT_FIELD_KEYS: [&str; 31] = [
    "opentelemetry_trace_export_schema_version",
    "opentelemetry_trace_export_report_id",
    "opentelemetry_trace_export_gar_id",
    "opentelemetry_trace_export_docs_ref",
    "opentelemetry_trace_export_traces_ref",
    "opentelemetry_trace_export_common_ref",
    "opentelemetry_trace_export_otlp_spec_ref",
    "opentelemetry_trace_export_otlp_exporter_ref",
    "opentelemetry_trace_export_schema_url_base_placeholder",
    "opentelemetry_trace_export_row_count",
    "opentelemetry_trace_export_row_order",
    "opentelemetry_trace_export_trace_export_enabled",
    "opentelemetry_trace_export_metric_export_enabled",
    "opentelemetry_trace_export_log_export_enabled",
    "opentelemetry_trace_export_otlp_exporter_configured",
    "opentelemetry_trace_export_network_exporter_enabled",
    "opentelemetry_trace_export_collector_configured",
    "opentelemetry_trace_export_sdk_dependency_added",
    "opentelemetry_trace_export_runtime_collection_enabled",
    "opentelemetry_trace_export_trace_emitted",
    "opentelemetry_trace_export_metric_emitted",
    "opentelemetry_trace_export_log_emitted",
    "opentelemetry_trace_export_network_call_performed",
    "opentelemetry_trace_export_attribute_allowlist_required",
    "opentelemetry_trace_export_redaction_policy_required",
    "opentelemetry_trace_export_retention_policy_required",
    "opentelemetry_trace_export_opt_in_required",
    "opentelemetry_trace_export_all_rows_report_only",
    "opentelemetry_trace_export_all_rows_no_fallback_no_external_engine",
    "opentelemetry_trace_export_no_export_side_effects",
    "opentelemetry_trace_export_claim_gate_status",
];

const OPENTELEMETRY_TRACE_EXPORT_SPAN_IDS: [&str; 9] = [
    "request_admission",
    "source_read",
    "compatibility_parse",
    "vortex_import",
    "vortex_scan",
    "operator_compute",
    "result_sink",
    "evidence_render",
    "claim_gate",
];

const OPENTELEMETRY_TRACE_EXPORT_SPAN_SUFFIXES: [&str; 19] = [
    "span_name",
    "span_kind",
    "timing_fields",
    "shardloom_attribute_allowlist",
    "redaction_policy",
    "sensitive_fields",
    "metric_refs",
    "span_status",
    "export_enabled",
    "span_emitted",
    "metric_emitted",
    "log_emitted",
    "network_exporter_enabled",
    "redaction_required",
    "retention_policy_required",
    "claim_gate_status",
    "claim_boundary",
    "fallback_attempted",
    "external_engine_invoked",
];

const EXTERNAL_EFFECT_BLOCKER_FIELD_KEYS: [&str; 15] = [
    "external_effect_blocker_matrix_schema_version",
    "external_effect_blocker_matrix_id",
    "external_effect_blocker_docs_ref",
    "external_effect_blocker_support_status_vocabulary",
    "external_effect_blocker_claim_gate_status",
    "external_effect_blocker_row_count",
    "external_effect_blocker_row_order",
    "external_effect_blocker_blocker_ids",
    "external_effect_blocker_required_evidence",
    "external_effect_blocker_all_effects_blocked",
    "external_effect_blocker_runtime_execution",
    "external_effect_blocker_credential_resolution_performed",
    "external_effect_blocker_network_probe_performed",
    "external_effect_blocker_fallback_attempted",
    "external_effect_blocker_external_engine_invoked",
];

const EXTERNAL_EFFECT_BLOCKER_ROW_IDS: [&str; 12] = [
    "sql_udf",
    "rust_udf",
    "wasm_udf",
    "python_udf",
    "external_service_udf",
    "api_call",
    "llm_call",
    "embedding_generation",
    "vector_search",
    "plugin_execution",
    "media_extraction",
    "network_egress",
];

const EXTERNAL_EFFECT_BLOCKER_ROW_SUFFIXES: [&str; 17] = [
    "family",
    "operation",
    "support_status",
    "permission_status",
    "effect_status",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "credential_required",
    "network_required",
    "sandbox_required",
    "model_or_embedding_call",
    "data_egress_possible",
    "materialization_boundary_required",
    "runtime_execution",
    "effect_executed",
    "claim_boundary",
];

const EFFECTFUL_OPERATION_ADMISSION_FIELD_KEYS: [&str; 21] = [
    "effectful_operation_admission_matrix_schema_version",
    "effectful_operation_admission_matrix_id",
    "effectful_operation_admission_docs_ref",
    "effectful_operation_admission_support_status_vocabulary",
    "effectful_operation_admission_claim_gate_status",
    "effectful_operation_admission_row_count",
    "effectful_operation_admission_admitted_local_fixture_count",
    "effectful_operation_admission_metadata_only_count",
    "effectful_operation_admission_blocked_count",
    "effectful_operation_admission_row_order",
    "effectful_operation_admission_blocker_ids",
    "effectful_operation_admission_required_evidence",
    "effectful_operation_admission_all_external_and_sandboxed_paths_blocked",
    "effectful_operation_admission_credential_resolution_performed",
    "effectful_operation_admission_network_probe_performed",
    "effectful_operation_admission_dynamic_loading_performed",
    "effectful_operation_admission_extension_code_executed",
    "effectful_operation_admission_external_effect_executed",
    "effectful_operation_admission_dependency_expansion_allowed",
    "effectful_operation_admission_fallback_attempted",
    "effectful_operation_admission_external_engine_invoked",
];

const EFFECTFUL_OPERATION_ADMISSION_ROW_IDS: [&str; 8] = [
    "local_sqlite_import_export",
    "typed_extension_manifest_inspection",
    "deterministic_scalar_udf_fixture",
    "network_database_connectors",
    "rest_flight_adbc_connectors",
    "python_udf",
    "wasm_or_dynamic_plugin_udf",
    "llm_api_embedding_vector_effects",
];

const EFFECTFUL_OPERATION_ADMISSION_ROW_SUFFIXES: [&str; 20] = [
    "family",
    "operation",
    "support_status",
    "admission_scope",
    "permission_status",
    "effect_status",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "credential_required",
    "network_required",
    "sandbox_required",
    "local_filesystem_io_allowed",
    "runtime_fixture_available",
    "extension_code_executed",
    "dynamic_loading_performed",
    "external_effect_executed",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const EXTENSION_MANIFEST_EFFECT_FIELD_KEYS: [&str; 21] = [
    "extension_manifest_effect_matrix_schema_version",
    "extension_manifest_effect_matrix_id",
    "extension_manifest_effect_docs_ref",
    "extension_manifest_effect_support_status_vocabulary",
    "extension_manifest_effect_claim_gate_status",
    "extension_manifest_effect_row_count",
    "extension_manifest_effect_row_order",
    "extension_manifest_effect_blocker_ids",
    "extension_manifest_effect_required_evidence",
    "extension_manifest_effect_all_runtime_blocked",
    "extension_manifest_effect_all_external_effects_blocked",
    "extension_manifest_effect_runtime_execution",
    "extension_manifest_effect_extension_code_executed",
    "extension_manifest_effect_dynamic_loading",
    "extension_manifest_effect_udf_execution",
    "extension_manifest_effect_external_effect_executed",
    "extension_manifest_effect_credential_resolution_performed",
    "extension_manifest_effect_network_probe_performed",
    "extension_manifest_effect_dependency_expansion_allowed",
    "extension_manifest_effect_fallback_attempted",
    "extension_manifest_effect_external_engine_invoked",
];

const EXTENSION_MANIFEST_EFFECT_ROW_IDS: [&str; 14] = [
    "metadata_only_manifest",
    "sql_frontend_extension",
    "rust_udf_extension",
    "wasm_udf_extension",
    "python_udf_extension",
    "encoded_kernel_extension",
    "translation_sink_extension",
    "connector_extension",
    "object_store_provider_extension",
    "catalog_provider_extension",
    "api_llm_effect_provider",
    "embedding_vector_provider",
    "observability_exporter_extension",
    "benchmark_provider_extension",
];

const EXTENSION_MANIFEST_EFFECT_ROW_SUFFIXES: [&str; 21] = [
    "extension_type",
    "support_status",
    "manifest_status",
    "required_permissions",
    "sandbox_policy",
    "effect_metadata",
    "materialization_boundary_required",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "runtime_execution",
    "extension_code_executed",
    "dynamic_loading",
    "udf_execution",
    "external_effect_executed",
    "credential_resolution_performed",
    "network_probe_performed",
    "dependency_expansion_allowed",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const PLUGIN_ABI_UDF_SANDBOX_BLOCKER_FIELD_KEYS: [&str; 24] = [
    "plugin_abi_udf_sandbox_blocker_schema_version",
    "plugin_abi_udf_sandbox_blocker_id",
    "plugin_abi_udf_sandbox_blocker_docs_ref",
    "plugin_abi_udf_sandbox_blocker_support_status",
    "plugin_abi_udf_sandbox_blocker_claim_gate_status",
    "plugin_abi_udf_sandbox_blocker_row_count",
    "plugin_abi_udf_sandbox_blocker_row_order",
    "plugin_abi_udf_sandbox_blocker_blocker_ids",
    "plugin_abi_udf_sandbox_blocker_required_evidence",
    "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
    "plugin_abi_udf_sandbox_blocker_abi_loading_supported",
    "plugin_abi_udf_sandbox_blocker_dynamic_loading_performed",
    "plugin_abi_udf_sandbox_blocker_extension_code_executed",
    "plugin_abi_udf_sandbox_blocker_udf_execution_performed",
    "plugin_abi_udf_sandbox_blocker_sandbox_evidence_required",
    "plugin_abi_udf_sandbox_blocker_sandbox_enforced",
    "plugin_abi_udf_sandbox_blocker_permission_policy_enforced",
    "plugin_abi_udf_sandbox_blocker_runtime_execution",
    "plugin_abi_udf_sandbox_blocker_external_effect_executed",
    "plugin_abi_udf_sandbox_blocker_credential_resolution_performed",
    "plugin_abi_udf_sandbox_blocker_network_probe_performed",
    "plugin_abi_udf_sandbox_blocker_dependency_expansion_allowed",
    "plugin_abi_udf_sandbox_blocker_fallback_attempted",
    "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
];

const PLUGIN_ABI_UDF_SANDBOX_BLOCKER_ROW_IDS: [&str; 12] = [
    "abi_contract_inventory",
    "dynamic_library_loading",
    "rust_native_udf",
    "wasm_udf",
    "python_udf",
    "sql_defined_udf",
    "external_service_udf",
    "table_function_udf",
    "plugin_lifecycle_transition",
    "sandbox_evidence_binding",
    "license_provenance_attestation",
    "unsupported_diagnostics",
];

const PLUGIN_ABI_UDF_SANDBOX_BLOCKER_ROW_SUFFIXES: [&str; 21] = [
    "plugin_surface",
    "support_status",
    "abi_status",
    "sandbox_requirement",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "user_visible_surface",
    "dynamic_loading_performed",
    "extension_code_executed",
    "udf_execution_performed",
    "sandbox_enforced",
    "permission_policy_enforced",
    "runtime_execution",
    "external_effect_executed",
    "credential_resolution_performed",
    "network_probe_performed",
    "dependency_expansion_allowed",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const CREDENTIAL_POLICY_GATE_FIELD_KEYS: [&str; 23] = [
    "credential_policy_gate_schema_version",
    "credential_policy_gate_id",
    "credential_policy_gate_docs_ref",
    "credential_policy_gate_support_status",
    "credential_policy_gate_claim_gate_status",
    "credential_policy_gate_row_count",
    "credential_policy_gate_row_order",
    "credential_policy_gate_blocker_ids",
    "credential_policy_gate_required_evidence",
    "credential_policy_gate_all_credential_runtime_blocked",
    "credential_policy_gate_credential_references_only",
    "credential_policy_gate_credential_resolution_performed",
    "credential_policy_gate_secret_loading_performed",
    "credential_policy_gate_secret_value_materialized",
    "credential_policy_gate_runtime_permission_checks_enforced",
    "credential_policy_gate_workspace_policy_enforced",
    "credential_policy_gate_production_policy_runtime_supported",
    "credential_policy_gate_redaction_required",
    "credential_policy_gate_audit_required",
    "credential_policy_gate_network_probe_performed",
    "credential_policy_gate_external_effect_executed",
    "credential_policy_gate_fallback_attempted",
    "credential_policy_gate_external_engine_invoked",
];

const CREDENTIAL_POLICY_GATE_ROW_IDS: [&str; 10] = [
    "credential_reference_inventory",
    "secret_loading",
    "environment_secret_provider",
    "file_secret_provider",
    "external_secret_manager_provider",
    "cloud_iam_provider",
    "workspace_policy",
    "runtime_permission_check",
    "redaction_policy",
    "unsupported_diagnostics",
];

const CREDENTIAL_POLICY_GATE_ROW_SUFFIXES: [&str; 19] = [
    "lifecycle_surface",
    "support_status",
    "default_policy",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "user_visible_surface",
    "credential_resolution_performed",
    "secret_loading_performed",
    "secret_value_materialized",
    "runtime_permission_check_enforced",
    "workspace_policy_enforced",
    "redaction_required",
    "audit_required",
    "network_probe_performed",
    "external_effect_executed",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const SANDBOX_GOVERNANCE_GATE_FIELD_KEYS: [&str; 29] = [
    "sandbox_governance_gate_schema_version",
    "sandbox_governance_gate_id",
    "sandbox_governance_gate_docs_ref",
    "sandbox_governance_gate_support_status",
    "sandbox_governance_gate_claim_gate_status",
    "sandbox_governance_gate_row_count",
    "sandbox_governance_gate_row_order",
    "sandbox_governance_gate_blocker_ids",
    "sandbox_governance_gate_required_evidence",
    "sandbox_governance_gate_all_sandbox_runtime_blocked",
    "sandbox_governance_gate_deny_by_default",
    "sandbox_governance_gate_sandbox_runtime_supported",
    "sandbox_governance_gate_sandbox_process_spawned",
    "sandbox_governance_gate_extension_code_executed",
    "sandbox_governance_gate_udf_code_executed",
    "sandbox_governance_gate_filesystem_access_allowed",
    "sandbox_governance_gate_network_access_allowed",
    "sandbox_governance_gate_environment_access_allowed",
    "sandbox_governance_gate_secret_access_allowed",
    "sandbox_governance_gate_process_execution_allowed",
    "sandbox_governance_gate_resource_limits_enforced",
    "sandbox_governance_gate_timeout_enforced",
    "sandbox_governance_gate_audit_required",
    "sandbox_governance_gate_audit_log_runtime_supported",
    "sandbox_governance_gate_deterministic_unsupported_diagnostics",
    "sandbox_governance_gate_production_governance_runtime_supported",
    "sandbox_governance_gate_external_effect_executed",
    "sandbox_governance_gate_fallback_attempted",
    "sandbox_governance_gate_external_engine_invoked",
];

const SANDBOX_GOVERNANCE_GATE_ROW_IDS: [&str; 11] = [
    "sandbox_profile_inventory",
    "filesystem_permission",
    "network_permission",
    "environment_access",
    "secret_access",
    "process_execution",
    "resource_limits",
    "execution_timeout",
    "audit_log",
    "dependency_isolation",
    "unsupported_diagnostics",
];

const SANDBOX_GOVERNANCE_GATE_ROW_SUFFIXES: [&str; 20] = [
    "readiness_surface",
    "support_status",
    "default_policy",
    "blocker_id",
    "diagnostic_code",
    "required_evidence",
    "user_visible_surface",
    "sandbox_enforced",
    "filesystem_access_allowed",
    "network_access_allowed",
    "environment_access_allowed",
    "secret_access_allowed",
    "process_execution_allowed",
    "resource_limits_enforced",
    "timeout_enforced",
    "audit_log_emitted",
    "external_effect_executed",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const UNSTRUCTURED_ADAPTER_CAPABILITY_FIELD_KEYS: [&str; 14] = [
    "unstructured_adapter_capability_schema_version",
    "unstructured_adapter_capability_matrix_id",
    "unstructured_adapter_capability_docs_ref",
    "unstructured_adapter_capability_support_status_vocabulary",
    "unstructured_adapter_capability_claim_gate_status",
    "unstructured_adapter_capability_row_count",
    "unstructured_adapter_capability_row_order",
    "unstructured_adapter_capability_runtime_execution",
    "unstructured_adapter_capability_source_io_performed",
    "unstructured_adapter_capability_sink_io_performed",
    "unstructured_adapter_capability_model_call_performed",
    "unstructured_adapter_capability_network_probe_performed",
    "unstructured_adapter_capability_fallback_attempted",
    "unstructured_adapter_capability_external_engine_invoked",
];

const UNSTRUCTURED_ADAPTER_CAPABILITY_ROW_IDS: [&str; 11] = [
    "document_reference",
    "text_extraction",
    "image_audio_video",
    "embedding_vector_generation",
    "vector_search",
    "vortex_turboquant_vector_encoding",
    "universal_file_adapter",
    "database_warehouse_adapter",
    "object_store_table_adapter",
    "event_api_saas_adapter",
    "source_sink_metadata",
];

const UNSTRUCTURED_ADAPTER_CAPABILITY_ROW_SUFFIXES: [&str; 14] = [
    "family",
    "surface",
    "support_status",
    "runtime_execution",
    "source_io_performed",
    "sink_io_performed",
    "metadata_only",
    "credential_required",
    "network_required",
    "model_call_required",
    "external_effect_blocker_id",
    "blocker_id",
    "required_evidence",
    "claim_boundary",
];

const DATAFRAME_NOTEBOOK_PACKAGE_READINESS_FIELD_KEYS: [&str; 24] = [
    "dataframe_notebook_package_readiness_schema_version",
    "dataframe_notebook_package_readiness_report_id",
    "dataframe_notebook_package_readiness_docs_ref",
    "dataframe_notebook_package_readiness_source_refs",
    "dataframe_notebook_package_readiness_support_status_vocabulary",
    "dataframe_notebook_package_readiness_row_count",
    "dataframe_notebook_package_readiness_row_order",
    "dataframe_notebook_package_readiness_ready_local_count",
    "dataframe_notebook_package_readiness_smoke_supported_count",
    "dataframe_notebook_package_readiness_report_only_count",
    "dataframe_notebook_package_readiness_blocked_count",
    "dataframe_notebook_package_readiness_local_install_smoke_supported",
    "dataframe_notebook_package_readiness_installed_package_smoke_distinct_from_runtime_support",
    "dataframe_notebook_package_readiness_dataframe_runtime_supported",
    "dataframe_notebook_package_readiness_notebook_runtime_supported",
    "dataframe_notebook_package_readiness_package_publication_ready",
    "dataframe_notebook_package_readiness_package_publication_claim_allowed",
    "dataframe_notebook_package_readiness_dataframe_runtime_claim_allowed",
    "dataframe_notebook_package_readiness_notebook_runtime_claim_allowed",
    "dataframe_notebook_package_readiness_fallback_attempted",
    "dataframe_notebook_package_readiness_external_engine_invoked",
    "dataframe_notebook_package_readiness_all_rows_no_runtime_claims",
    "dataframe_notebook_package_readiness_claim_gate_status",
    "dataframe_notebook_package_readiness_claim_boundary",
];

const DATAFRAME_NOTEBOOK_PACKAGE_READINESS_ROW_IDS: [&str; 6] = [
    "python_package_metadata",
    "editable_install_smoke",
    "dataframe_method_matrix",
    "notebook_display_surface",
    "public_package_publication",
    "unsupported_diagnostics",
];

const DATAFRAME_NOTEBOOK_PACKAGE_READINESS_ROW_SUFFIXES: [&str; 14] = [
    "family",
    "surface",
    "support_status",
    "local_install_smoke",
    "package_publication_allowed",
    "dataframe_runtime_supported",
    "notebook_runtime_supported",
    "deterministic_diagnostic_code",
    "blocker_id",
    "required_evidence",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_boundary",
];

const SQL_FIELD_KEYS: [&str; 55] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "sql_feature_count",
    "planned_count",
    "certified_count",
    "planner_readiness_schema_version",
    "planner_readiness_matrix_id",
    "planner_readiness_report_ref",
    "planner_readiness_docs_ref",
    "planner_readiness_support_status_vocabulary",
    "planner_readiness_claim_gate_status",
    "planner_readiness_row_count",
    "planner_readiness_row_order",
    "planner_readiness_sql_row_order",
    "planner_readiness_dataframe_row_order",
    "planner_readiness_unsupported_diagnostic_codes",
    "planner_readiness_blocker_ids",
    "planner_readiness_required_evidence",
    "planner_readiness_parser_executed",
    "planner_readiness_binder_executed",
    "planner_readiness_planner_executed",
    "planner_readiness_runtime_execution",
    "planner_readiness_dataframe_runtime",
    "planner_readiness_external_engine_invoked",
    "planner_readiness_fallback_attempted",
    "planner_readiness_deterministic_diagnostics_present",
    "sql_local_source_smoke_schema_version",
    "sql_local_source_smoke_command",
    "sql_local_source_smoke_support_status",
    "sql_local_source_smoke_statement_shape",
    "sql_local_source_smoke_execution_mode",
    "sql_local_source_smoke_engine_mode",
    "sql_local_source_smoke_source_format",
    "sql_local_source_smoke_result_format",
    "sql_local_source_smoke_runtime_execution",
    "sql_local_source_smoke_parser_executed",
    "sql_local_source_smoke_binder_executed",
    "sql_local_source_smoke_planner_executed",
    "sql_local_source_smoke_source_io_performed",
    "sql_local_source_smoke_output_io_performed",
    "sql_local_source_smoke_object_store_io",
    "sql_local_source_smoke_external_engine_invoked",
    "sql_local_source_smoke_fallback_attempted",
    "sql_local_source_smoke_claim_gate_status",
    "sql_local_source_smoke_claim_boundary",
    "sql_local_source_smoke_blocked_shapes",
];

const SQL_FRONTEND_RUNTIME_LADDER_FIELD_KEYS: [&str; 24] = [
    "sql_frontend_runtime_ladder_schema_version",
    "sql_frontend_runtime_ladder_matrix_id",
    "sql_frontend_runtime_ladder_support_status_vocabulary",
    "sql_frontend_runtime_ladder_row_count",
    "sql_frontend_runtime_ladder_row_order",
    "sql_frontend_runtime_ladder_runtime_family_order",
    "sql_frontend_runtime_ladder_blocked_family_order",
    "sql_frontend_runtime_ladder_smoke_supported_count",
    "sql_frontend_runtime_ladder_blocked_count",
    "sql_frontend_runtime_ladder_blocker_ids",
    "sql_frontend_runtime_ladder_required_evidence",
    "sql_frontend_runtime_ladder_parser_executed",
    "sql_frontend_runtime_ladder_binder_executed",
    "sql_frontend_runtime_ladder_planner_executed",
    "sql_frontend_runtime_ladder_runtime_execution",
    "sql_frontend_runtime_ladder_dataframe_runtime",
    "sql_frontend_runtime_ladder_source_io_performed",
    "sql_frontend_runtime_ladder_output_io_performed",
    "sql_frontend_runtime_ladder_deterministic_diagnostics_present",
    "sql_frontend_runtime_ladder_fallback_attempted",
    "sql_frontend_runtime_ladder_external_engine_invoked",
    "sql_frontend_runtime_ladder_broad_sql_claim_allowed",
    "sql_frontend_runtime_ladder_claim_gate_status",
    "sql_frontend_runtime_ladder_claim_boundary",
];

const SQL_FRONTEND_RUNTIME_LADDER_ROW_IDS: [&str; 13] = [
    "local_source_projection_filter_limit",
    "local_source_predicate_expression_ladder",
    "local_source_aggregate_group_having",
    "local_source_order_topn",
    "local_source_join_ladder",
    "local_source_window_ladder",
    "local_source_output_fanout",
    "source_free_sql_generated_output",
    "broad_sql_parse_bind_plan_execute",
    "catalog_cte_setop_recursive_sql",
    "correlated_and_broad_subquery_sql",
    "object_store_table_sql",
    "fallback_engine_sql",
];

const SQL_FRONTEND_RUNTIME_LADDER_ROW_SUFFIXES: [&str; 20] = [
    "syntax_family",
    "surface",
    "support_status",
    "parser_executed",
    "binder_executed",
    "planner_executed",
    "runtime_execution",
    "dataframe_runtime",
    "source_io_performed",
    "output_io_performed",
    "materialization_required",
    "deterministic_diagnostics",
    "blocker_id",
    "unsupported_diagnostic_code",
    "required_evidence",
    "evidence_command_refs",
    "claim_gate_status",
    "claim_boundary",
    "fallback_attempted",
    "external_engine_invoked",
];

fn with_generated_source_fields(base_keys: &[&'static str]) -> Vec<&'static str> {
    base_keys
        .iter()
        .copied()
        .chain(GENERATED_SOURCE_FIELD_KEYS)
        .collect()
}

fn append_sql_frontend_runtime_ladder_keys(keys: &mut Vec<String>) {
    keys.extend(
        SQL_FRONTEND_RUNTIME_LADDER_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in SQL_FRONTEND_RUNTIME_LADDER_ROW_IDS {
        keys.extend(
            SQL_FRONTEND_RUNTIME_LADDER_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("sql_frontend_runtime_ladder_row_{row_id}_{suffix}")),
        );
    }
}

fn with_sql_frontend_runtime_ladder_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = Vec::new();
    for key in base_keys {
        keys.push((*key).to_string());
        if *key == "planner_readiness_deterministic_diagnostics_present" {
            append_sql_frontend_runtime_ladder_keys(&mut keys);
        }
    }
    keys
}

fn with_sql_generated_source_alignment_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = with_sql_frontend_runtime_ladder_fields(base_keys);
    append_generated_source_contract_keys(&mut keys);
    append_generated_source_api_admission_keys(&mut keys);
    append_generated_source_alignment_keys(&mut keys);
    keys
}

fn with_openlineage_facet_mapping_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys: Vec<String> = base_keys.iter().copied().map(str::to_string).collect();
    keys.extend(
        OPENLINEAGE_FACET_MAPPING_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in OPENLINEAGE_FACET_MAPPING_ROW_IDS {
        keys.extend(
            OPENLINEAGE_FACET_MAPPING_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("openlineage_facet_mapping_row_{row_id}_{suffix}")),
        );
    }
    keys
}

fn with_observability_contract_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = with_openlineage_facet_mapping_fields(base_keys);
    keys.extend(
        OPENTELEMETRY_TRACE_EXPORT_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for span_id in OPENTELEMETRY_TRACE_EXPORT_SPAN_IDS {
        keys.extend(
            OPENTELEMETRY_TRACE_EXPORT_SPAN_SUFFIXES
                .into_iter()
                .map(|suffix| format!("opentelemetry_trace_export_span_{span_id}_{suffix}")),
        );
    }
    keys
}

fn with_external_effect_blocker_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys: Vec<String> = base_keys.iter().copied().map(str::to_string).collect();
    append_external_effect_and_admission_keys(&mut keys);
    keys
}

fn append_external_effect_and_admission_keys(keys: &mut Vec<String>) {
    append_external_effect_blocker_keys(keys);
    append_effectful_operation_admission_keys(keys);
}

fn append_external_effect_blocker_keys(keys: &mut Vec<String>) {
    keys.extend(
        EXTERNAL_EFFECT_BLOCKER_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in EXTERNAL_EFFECT_BLOCKER_ROW_IDS {
        keys.extend(
            EXTERNAL_EFFECT_BLOCKER_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("external_effect_blocker_row_{row_id}_{suffix}")),
        );
    }
}

fn append_effectful_operation_admission_keys(keys: &mut Vec<String>) {
    keys.extend(
        EFFECTFUL_OPERATION_ADMISSION_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in EFFECTFUL_OPERATION_ADMISSION_ROW_IDS {
        keys.extend(
            EFFECTFUL_OPERATION_ADMISSION_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("effectful_operation_admission_row_{row_id}_{suffix}")),
        );
    }
}

fn append_extension_manifest_effect_keys(keys: &mut Vec<String>) {
    keys.extend(
        EXTENSION_MANIFEST_EFFECT_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in EXTENSION_MANIFEST_EFFECT_ROW_IDS {
        keys.extend(
            EXTENSION_MANIFEST_EFFECT_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("extension_manifest_effect_row_{row_id}_{suffix}")),
        );
    }
}

fn append_plugin_abi_udf_sandbox_blocker_keys(keys: &mut Vec<String>) {
    keys.extend(
        PLUGIN_ABI_UDF_SANDBOX_BLOCKER_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in PLUGIN_ABI_UDF_SANDBOX_BLOCKER_ROW_IDS {
        keys.extend(
            PLUGIN_ABI_UDF_SANDBOX_BLOCKER_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("plugin_abi_udf_sandbox_blocker_row_{row_id}_{suffix}")),
        );
    }
}

fn with_extension_manifest_effect_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = with_external_effect_blocker_fields(base_keys);
    append_extension_manifest_effect_keys(&mut keys);
    append_plugin_abi_udf_sandbox_blocker_keys(&mut keys);
    keys
}

fn append_credential_policy_gate_keys(keys: &mut Vec<String>) {
    keys.extend(
        CREDENTIAL_POLICY_GATE_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in CREDENTIAL_POLICY_GATE_ROW_IDS {
        keys.extend(
            CREDENTIAL_POLICY_GATE_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("credential_policy_gate_row_{row_id}_{suffix}")),
        );
    }
}

fn append_sandbox_governance_gate_keys(keys: &mut Vec<String>) {
    keys.extend(
        SANDBOX_GOVERNANCE_GATE_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in SANDBOX_GOVERNANCE_GATE_ROW_IDS {
        keys.extend(
            SANDBOX_GOVERNANCE_GATE_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("sandbox_governance_gate_row_{row_id}_{suffix}")),
        );
    }
}

fn with_security_governance_policy_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = with_extension_manifest_effect_fields(base_keys);
    append_credential_policy_gate_keys(&mut keys);
    append_sandbox_governance_gate_keys(&mut keys);
    keys
}

fn append_unstructured_adapter_capability_keys(keys: &mut Vec<String>) {
    keys.extend(
        UNSTRUCTURED_ADAPTER_CAPABILITY_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in UNSTRUCTURED_ADAPTER_CAPABILITY_ROW_IDS {
        keys.extend(
            UNSTRUCTURED_ADAPTER_CAPABILITY_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("unstructured_adapter_capability_row_{row_id}_{suffix}")),
        );
    }
}

fn append_dataframe_notebook_package_readiness_keys(keys: &mut Vec<String>) {
    keys.extend(
        DATAFRAME_NOTEBOOK_PACKAGE_READINESS_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in DATAFRAME_NOTEBOOK_PACKAGE_READINESS_ROW_IDS {
        keys.extend(
            DATAFRAME_NOTEBOOK_PACKAGE_READINESS_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| {
                    format!("dataframe_notebook_package_readiness_row_{row_id}_{suffix}")
                }),
        );
    }
}

fn append_generated_source_contract_keys(keys: &mut Vec<String>) {
    keys.extend(GENERATED_SOURCE_FIELD_KEYS.into_iter().map(str::to_string));
}

fn append_generated_source_api_admission_keys(keys: &mut Vec<String>) {
    keys.extend(
        GENERATED_SOURCE_API_ADMISSION_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in GENERATED_SOURCE_API_ADMISSION_ROW_IDS {
        keys.extend(
            GENERATED_SOURCE_API_ADMISSION_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("{row_id}_{suffix}")),
        );
    }
}

fn append_generated_source_alignment_keys(keys: &mut Vec<String>) {
    keys.extend(
        GENERATED_SOURCE_EVIDENCE_ALIGNMENT_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in GENERATED_SOURCE_EVIDENCE_ALIGNMENT_ROW_IDS {
        keys.extend(
            GENERATED_SOURCE_EVIDENCE_ALIGNMENT_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("generated_source_evidence_alignment_row_{row_id}_{suffix}")),
        );
    }
}

fn with_dataframe_notebook_package_readiness_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys: Vec<String> = base_keys.iter().copied().map(str::to_string).collect();
    append_dataframe_notebook_package_readiness_keys(&mut keys);
    keys
}

fn with_dataframe_notebook_package_and_generated_source_fields(
    base_keys: &[&'static str],
) -> Vec<String> {
    let mut keys = with_dataframe_notebook_package_readiness_fields(base_keys);
    append_generated_source_contract_keys(&mut keys);
    keys
}

fn with_dataframe_notebook_package_and_generated_source_alignment_fields(
    base_keys: &[&'static str],
) -> Vec<String> {
    let mut keys = with_dataframe_notebook_package_and_generated_source_fields(base_keys);
    append_generated_source_api_admission_keys(&mut keys);
    append_generated_source_alignment_keys(&mut keys);
    keys
}

fn append_command_registry_keys(keys: &mut Vec<String>) {
    keys.extend(COMMAND_REGISTRY_FIELD_KEYS.into_iter().map(str::to_string));
    for row_id in command_registry_row_field_ids() {
        keys.extend(
            COMMAND_REGISTRY_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("command_registry_row_{row_id}_{suffix}")),
        );
    }
}

fn append_evidence_schema_registry_keys(keys: &mut Vec<String>) {
    keys.extend(
        EVIDENCE_SCHEMA_REGISTRY_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for surface_id in EVIDENCE_SCHEMA_SURFACE_IDS {
        keys.extend(
            EVIDENCE_SCHEMA_SURFACE_SUFFIXES
                .into_iter()
                .map(|suffix| format!("evidence_schema_surface_{surface_id}_{suffix}")),
        );
        for field_id in evidence_schema_payload_field_ids(surface_id) {
            keys.extend(
                EVIDENCE_SCHEMA_FIELD_SUFFIXES.into_iter().map(|suffix| {
                    format!("evidence_schema_field_{surface_id}_{field_id}_{suffix}")
                }),
            );
        }
    }
}

fn evidence_schema_payload_field_ids(surface_id: &str) -> Vec<String> {
    let const_name = match surface_id {
        "execution_mode_selection_report" => "EXECUTION_MODE_SELECTION_REPORT_PAYLOAD_KEYS",
        "compute_flow_evidence" => "COMPUTE_FLOW_EVIDENCE_PAYLOAD_KEYS",
        "execution_certificate_report" => "EXECUTION_CERTIFICATE_REPORT_PAYLOAD_KEYS",
        "native_io_report" => "NATIVE_IO_REPORT_PAYLOAD_KEYS",
        "benchmark_plan_report" => "BENCHMARK_PLAN_REPORT_PAYLOAD_KEYS",
        "benchmark_constitution_report" => "BENCHMARK_CONSTITUTION_REPORT_PAYLOAD_KEYS",
        "benchmark_claim_evidence_report" => "BENCHMARK_CLAIM_EVIDENCE_REPORT_PAYLOAD_KEYS",
        "compute_capability_matrix_report" => "COMPUTE_CAPABILITY_MATRIX_REPORT_PAYLOAD_KEYS",
        _ => panic!("unknown evidence schema surface {surface_id}"),
    };
    let source = include_str!("../src/typed_envelope.rs");
    let marker = format!("const {const_name}: &[&str] = &[");
    let (_, after_marker) = source
        .split_once(&marker)
        .expect("payload const marker exists");
    let (array, _) = after_marker
        .split_once("];")
        .expect("payload const terminator exists");
    array
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let rest = trimmed.strip_prefix('"')?;
            let (field, _) = rest.split_once('"')?;
            Some(field.replace('-', "_"))
        })
        .collect()
}

fn command_registry_row_field_ids() -> Vec<String> {
    let source = include_str!("../src/command_registry.rs");
    let (_, after_marker) = source
        .split_once("pub(crate) const REGISTERED_COMMANDS: &[&str] = &[")
        .expect("registered commands array marker");
    let (array, _) = after_marker
        .split_once("];")
        .expect("registered commands array terminator");
    array
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let rest = trimmed.strip_prefix('"')?;
            let (command, _) = rest.split_once('"')?;
            Some(command.replace('-', "_"))
        })
        .collect()
}

fn with_dataframe_notebook_package_command_registry_wrapper_and_unstructured_fields(
    base_keys: &[&'static str],
) -> Vec<String> {
    let mut keys = with_dataframe_notebook_package_and_generated_source_alignment_fields(base_keys);
    append_command_registry_keys(&mut keys);
    append_evidence_schema_registry_keys(&mut keys);
    keys.extend(
        WRAPPER_CONNECTOR_REGISTRY_FIELD_KEYS
            .into_iter()
            .map(str::to_string),
    );
    for row_id in WRAPPER_CONNECTOR_REGISTRY_ROW_IDS {
        keys.extend(
            WRAPPER_CONNECTOR_REGISTRY_ROW_SUFFIXES
                .into_iter()
                .map(|suffix| format!("wrapper_connector_registry_row_{row_id}_{suffix}")),
        );
    }
    append_unstructured_adapter_capability_keys(&mut keys);
    keys
}

fn with_external_effect_and_unstructured_adapter_fields(base_keys: &[&'static str]) -> Vec<String> {
    let mut keys = with_external_effect_blocker_fields(base_keys);
    append_unstructured_adapter_capability_keys(&mut keys);
    keys
}

const FUNCTION_FIELD_KEYS: [&str; 20] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "function_coverage_report_id",
    "function_group_count",
    "planned_count",
    "native_count",
    "partial_count",
    "unsupported_count",
    "encoded_capable_count",
    "selection_vector_supported_count",
    "materialization_required_count",
];

const ENGINE_FIELD_KEYS: [&str; 107] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "external_effects_executed",
    "data_read",
    "write_io",
    "no_runtime",
    "no_fallback",
    "no_effects",
    "engine_capability_schema_version",
    "engine_capability_report_id",
    "engine_mode_vocabulary",
    "boundedness_vocabulary",
    "update_mode_vocabulary",
    "output_mode_vocabulary",
    "engine_mode_count",
    "partially_supported_engine_count",
    "planned_engine_count",
    "live_hybrid_claim_blocked_count",
    "severity",
    "blocker_ids",
    "required_evidence",
    "suggested_next_action",
    "future_rest_view",
    "streaming_capability_matrix_schema_version",
    "streaming_capability_matrix_report_id",
    "streaming_capability_matrix_status",
    "streaming_capability_matrix_claim_gate_status",
    "streaming_capability_matrix_row_count",
    "streaming_capability_matrix_blocked_row_count",
    "streaming_capability_matrix_report_only_row_count",
    "streaming_capability_matrix_fixture_smoke_row_count",
    "streaming_capability_matrix_materialization_row_count",
    "streaming_capability_matrix_row_order",
    "streaming_capability_matrix_family_order",
    "streaming_capability_matrix_diagnostic_code_order",
    "streaming_capability_matrix_all_rows_have_support_status",
    "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
    "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
    "streaming_capability_matrix_runtime_execution",
    "streaming_capability_matrix_data_read",
    "streaming_capability_matrix_object_store_io",
    "streaming_capability_matrix_write_io",
    "streaming_capability_matrix_fallback_attempted",
    "streaming_capability_matrix_external_engine_invoked",
    "live_hybrid_fabric_gate_schema_version",
    "live_hybrid_fabric_gate_report_id",
    "live_hybrid_fabric_gate_row_count",
    "live_hybrid_fabric_gate_row_order",
    "live_hybrid_fabric_gate_blocked_row_count",
    "live_hybrid_fabric_gate_report_only_row_count",
    "live_hybrid_fabric_gate_fixture_smoke_row_count",
    "live_hybrid_fabric_gate_blocker_ids",
    "live_hybrid_fabric_gate_required_evidence",
    "live_hybrid_fabric_gate_claim_boundary",
    "live_hybrid_fabric_gate_claim_gate_status",
    "live_hybrid_fabric_gate_freshness_claim_allowed",
    "live_hybrid_fabric_gate_exactly_once_claim_allowed",
    "live_hybrid_fabric_gate_production_live_claim_allowed",
    "live_hybrid_fabric_gate_production_hybrid_claim_allowed",
    "live_hybrid_fabric_gate_object_store_runtime_supported",
    "live_hybrid_fabric_gate_broker_runtime_supported",
    "live_hybrid_fabric_gate_state_store_runtime_supported",
    "live_hybrid_fabric_gate_baseline_oracle_only",
    "live_hybrid_fabric_gate_fallback_attempted",
    "live_hybrid_fabric_gate_external_engine_invoked",
    "batch_support_status",
    "batch_production_claim_allowed",
    "batch_state_required",
    "batch_checkpoint_required",
    "batch_blocker_ids",
    "batch_severity",
    "batch_required_evidence",
    "batch_suggested_next_action",
    "batch_no_runtime",
    "batch_no_fallback",
    "batch_no_effects",
    "live_support_status",
    "live_production_claim_allowed",
    "live_state_required",
    "live_checkpoint_required",
    "live_blocker_ids",
    "live_severity",
    "live_required_evidence",
    "live_suggested_next_action",
    "live_no_runtime",
    "live_no_fallback",
    "live_no_effects",
    "hybrid_support_status",
    "hybrid_production_claim_allowed",
    "hybrid_state_required",
    "hybrid_checkpoint_required",
    "hybrid_blocker_ids",
    "hybrid_severity",
    "hybrid_required_evidence",
    "hybrid_suggested_next_action",
    "hybrid_no_runtime",
    "hybrid_no_fallback",
    "hybrid_no_effects",
];

const WORKFLOW_FIELD_KEYS: [&str; 48] = [
    "scope",
    "schema_version",
    "report_id",
    "capability_status",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "external_effects_executed",
    "data_read",
    "write_io",
    "no_runtime",
    "no_fallback",
    "no_effects",
    "represented_gates",
    "represented_surfaces",
    "future_rest_view",
    "workflow_state",
    "workflow_operation_count",
    "workflow_operation_names",
    "severity",
    "blocker_ids",
    "required_evidence",
    "suggested_next_action",
    "unsupported_diagnostic_surface",
    "etl_workflow_matrix_schema_version",
    "etl_workflow_matrix_id",
    "etl_workflow_row_order",
    "etl_workflow_row_count",
    "etl_workflow_supported_local_rows",
    "etl_workflow_supported_local_count",
    "etl_workflow_report_only_rows",
    "etl_workflow_report_only_count",
    "etl_workflow_blocked_rows",
    "etl_workflow_blocked_count",
    "etl_workflow_required_evidence",
    "etl_workflow_claim_boundary",
    "etl_workflow_claim_gate_status",
    "etl_workflow_fallback_attempted",
    "etl_workflow_external_engine_invoked",
    "etl_workflow_production_etl_claim_allowed",
    "etl_workflow_object_store_runtime_supported",
    "etl_workflow_table_lakehouse_runtime_supported",
];

const REMOTE_API_FIELD_KEYS: [&str; 32] = [
    "scope",
    "schema_version",
    "report_id",
    "capability_status",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "external_effects_executed",
    "data_read",
    "write_io",
    "no_runtime",
    "no_fallback",
    "no_effects",
    "represented_gates",
    "represented_surfaces",
    "future_rest_view",
    "remote_api_state",
    "remote_api_surface_count",
    "remote_api_surface_names",
    "severity",
    "blocker_ids",
    "required_evidence",
    "suggested_next_action",
    "unsupported_diagnostic_surface",
    "contract_surface",
    "event_surface",
];

const CROSS_CG_FIELD_KEYS: [&str; 54] = [
    "scope",
    "schema_version",
    "report_id",
    "capability_status",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "external_effects_executed",
    "data_read",
    "write_io",
    "no_runtime",
    "no_fallback",
    "no_effects",
    "represented_gates",
    "represented_surfaces",
    "future_rest_view",
    "parity_surface_count",
    "severity",
    "blocker_ids",
    "required_evidence",
    "suggested_next_action",
    "cg21_workflow_state",
    "cg21_workflow_severity",
    "cg21_workflow_blocker_ids",
    "cg21_workflow_required_evidence",
    "cg21_workflow_suggested_next_action",
    "cg21_workflow_diagnostic_surface",
    "cg21_workflow_no_runtime",
    "cg21_workflow_no_fallback",
    "cg21_workflow_no_effects",
    "cg22_engine_modes_state",
    "cg22_engine_modes_severity",
    "cg22_engine_modes_blocker_ids",
    "cg22_engine_modes_required_evidence",
    "cg22_engine_modes_suggested_next_action",
    "cg22_engine_modes_diagnostic_surface",
    "cg22_engine_modes_no_runtime",
    "cg22_engine_modes_no_fallback",
    "cg22_engine_modes_no_effects",
    "cg23_remote_api_state",
    "cg23_remote_api_severity",
    "cg23_remote_api_blocker_ids",
    "cg23_remote_api_required_evidence",
    "cg23_remote_api_suggested_next_action",
    "cg23_remote_api_diagnostic_surface",
    "cg23_remote_api_no_runtime",
    "cg23_remote_api_no_fallback",
    "cg23_remote_api_no_effects",
];

const OPERATOR_FIELD_KEYS: [&str; 185] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "operator_coverage_report_id",
    "operator_family_count",
    "operator_encoded_capable_count",
    "operator_native_decoded_count",
    "operator_planned_native_count",
    "operator_unsupported_count",
    "production_certified_count",
    "physical_operator_schema_version",
    "physical_operator_plan_id",
    "physical_operator_count",
    "physical_operator_ready_count",
    "physical_operator_missing_kernel_count",
    "physical_operator_unsupported_count",
    "physical_operator_fallback_execution_allowed",
    "physical_operator_runtime_execution",
    "physical_operator_execution_profile_schema_version",
    "physical_operator_execution_profile_count",
    "physical_operator_native_execution_level_count",
    "physical_operator_metadata_only_level_count",
    "physical_operator_encoded_native_level_count",
    "physical_operator_hybrid_native_level_count",
    "physical_operator_native_decoded_level_count",
    "physical_operator_reference_only_level_count",
    "physical_operator_row_materialization_level_count",
    "physical_operator_arrow_conversion_level_count",
    "physical_operator_fallback_level_count",
    "metadata_physical_kernel_schema_version",
    "metadata_physical_kernel_supported_primitives",
    "metadata_physical_kernel_contextual_only",
    "metadata_physical_kernel_requires_correctness_evidence",
    "metadata_physical_kernel_requires_memory_safety_evidence",
    "metadata_physical_kernel_requires_benchmark_for_production",
    "metadata_physical_kernel_data_read",
    "metadata_physical_kernel_data_decoded",
    "metadata_physical_kernel_data_materialized",
    "metadata_physical_kernel_object_store_io",
    "metadata_physical_kernel_write_io",
    "metadata_physical_kernel_spill_io",
    "metadata_physical_kernel_runtime_execution",
    "metadata_physical_kernel_fallback_execution_allowed",
    "metadata_count_kernel_admission_schema_version",
    "metadata_count_kernel_admission_contextual_only",
    "metadata_count_kernel_admission_operator_kind",
    "metadata_count_kernel_admission_required_kernel_kind",
    "metadata_count_kernel_admission_requires_metadata_kernel_evidence",
    "metadata_count_kernel_admission_requires_correctness_evidence",
    "metadata_count_kernel_admission_requires_memory_safety_evidence",
    "metadata_count_kernel_admission_requires_benchmark_for_production",
    "metadata_count_kernel_admission_runtime_execution",
    "metadata_count_kernel_admission_fallback_execution_allowed",
    "metadata_filter_kernel_admission_schema_version",
    "metadata_filter_kernel_admission_contextual_only",
    "metadata_filter_kernel_admission_operator_kind",
    "metadata_filter_kernel_admission_required_kernel_kind",
    "metadata_filter_kernel_admission_requires_metadata_kernel_evidence",
    "metadata_filter_kernel_admission_requires_correctness_evidence",
    "metadata_filter_kernel_admission_requires_memory_safety_evidence",
    "metadata_filter_kernel_admission_requires_benchmark_for_production",
    "metadata_filter_kernel_admission_runtime_execution",
    "metadata_filter_kernel_admission_fallback_execution_allowed",
    "metadata_projection_kernel_admission_schema_version",
    "metadata_projection_kernel_admission_contextual_only",
    "metadata_projection_kernel_admission_operator_kind",
    "metadata_projection_kernel_admission_required_kernel_kind",
    "metadata_projection_kernel_admission_requires_projection_readiness",
    "metadata_projection_kernel_admission_requires_correctness_evidence",
    "metadata_projection_kernel_admission_requires_memory_safety_evidence",
    "metadata_projection_kernel_admission_requires_benchmark_for_production",
    "metadata_projection_kernel_admission_runtime_execution",
    "metadata_projection_kernel_admission_fallback_execution_allowed",
    "encoded_projection_kernel_admission_schema_version",
    "encoded_projection_kernel_admission_contextual_only",
    "encoded_projection_kernel_admission_operator_kind",
    "encoded_projection_kernel_admission_required_kernel_kind",
    "encoded_projection_kernel_admission_requires_projection_readiness",
    "encoded_projection_kernel_admission_requires_encoded_column_path",
    "encoded_projection_kernel_admission_requires_correctness_evidence",
    "encoded_projection_kernel_admission_requires_memory_safety_evidence",
    "encoded_projection_kernel_admission_requires_benchmark_for_production",
    "encoded_projection_kernel_admission_runtime_execution",
    "encoded_projection_kernel_admission_fallback_execution_allowed",
    "encoded_count_physical_kernel_schema_version",
    "encoded_count_physical_kernel_id",
    "encoded_count_physical_kernel_supported_primitive",
    "encoded_count_physical_kernel_operator_kind",
    "encoded_count_physical_kernel_kernel_kind",
    "encoded_count_physical_kernel_execution_level",
    "encoded_count_physical_kernel_contextual_only",
    "encoded_count_physical_kernel_requires_execution_certificate",
    "encoded_count_physical_kernel_requires_correctness_evidence",
    "encoded_count_physical_kernel_requires_memory_safety_evidence",
    "encoded_count_physical_kernel_requires_benchmark_for_production",
    "encoded_count_physical_kernel_discovery_reads_data",
    "encoded_count_physical_kernel_evaluated_path_reads_data",
    "encoded_count_physical_kernel_runtime_execution",
    "encoded_count_physical_kernel_fallback_execution_allowed",
    "encoded_count_kernel_admission_schema_version",
    "encoded_count_kernel_admission_contextual_only",
    "encoded_count_kernel_admission_operator_kind",
    "encoded_count_kernel_admission_required_kernel_kind",
    "encoded_count_kernel_admission_requires_physical_kernel_evidence",
    "encoded_count_kernel_admission_requires_correctness_evidence",
    "encoded_count_kernel_admission_requires_memory_safety_evidence",
    "encoded_count_kernel_admission_requires_benchmark_for_production",
    "encoded_count_kernel_admission_runtime_execution",
    "encoded_count_kernel_admission_fallback_execution_allowed",
    "encoded_predicate_evaluation_schema_version",
    "encoded_predicate_evaluation_id",
    "encoded_predicate_evaluation_operator_kind",
    "encoded_predicate_evaluation_kernel_kind",
    "encoded_predicate_evaluation_execution_level",
    "encoded_predicate_evaluation_contextual_only",
    "encoded_predicate_evaluation_emits_selection_vectors",
    "encoded_predicate_evaluation_supports_metadata_proven_all",
    "encoded_predicate_evaluation_supports_metadata_proven_none",
    "encoded_predicate_evaluation_defers_inconclusive_to_encoded_values",
    "encoded_predicate_evaluation_discovery_reads_data",
    "encoded_predicate_evaluation_runtime_execution",
    "encoded_predicate_evaluation_fallback_execution_allowed",
    "selection_vector_filter_kernel_schema_version",
    "selection_vector_filter_kernel_id",
    "selection_vector_filter_kernel_operator_kind",
    "selection_vector_filter_kernel_kernel_kind",
    "selection_vector_filter_kernel_execution_level",
    "selection_vector_filter_kernel_contextual_only",
    "selection_vector_filter_kernel_requires_encoded_predicate_evaluation",
    "selection_vector_filter_kernel_requires_selection_vectors",
    "selection_vector_filter_kernel_requires_correctness_evidence",
    "selection_vector_filter_kernel_requires_memory_safety_evidence",
    "selection_vector_filter_kernel_requires_benchmark_for_production",
    "selection_vector_filter_kernel_discovery_reads_data",
    "selection_vector_filter_kernel_runtime_execution",
    "selection_vector_filter_kernel_fallback_execution_allowed",
    "selection_vector_filter_kernel_admission_schema_version",
    "selection_vector_filter_kernel_admission_contextual_only",
    "selection_vector_filter_kernel_admission_operator_kind",
    "selection_vector_filter_kernel_admission_required_kernel_kind",
    "selection_vector_filter_kernel_admission_requires_filter_kernel_evidence",
    "selection_vector_filter_kernel_admission_requires_correctness_evidence",
    "selection_vector_filter_kernel_admission_requires_memory_safety_evidence",
    "selection_vector_filter_kernel_admission_requires_benchmark_for_production",
    "selection_vector_filter_kernel_admission_runtime_execution",
    "selection_vector_filter_kernel_admission_fallback_execution_allowed",
    "encoded_count_local_guard_schema_version",
    "encoded_count_local_guard_id",
    "encoded_count_local_guard_accepted_approval_sources",
    "encoded_count_local_guard_local_execution_status",
    "encoded_count_local_guard_mode",
    "encoded_count_local_guard_layout_row_count_path_accepted",
    "encoded_count_local_guard_approved_local_scan_result_bridge_available",
    "encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report",
    "encoded_count_local_guard_returns_count_result",
    "encoded_count_local_guard_side_effect_free",
    "encoded_count_local_guard_data_read",
    "encoded_count_local_guard_data_decoded",
    "encoded_count_local_guard_data_materialized",
    "encoded_count_local_guard_runtime_execution",
    "encoded_count_local_guard_fallback_execution_allowed",
    "local_vortex_primitive_execution_schema_version",
    "local_vortex_primitive_execution_feature_gate",
    "local_vortex_primitive_execution_supported_primitives",
    "local_vortex_primitive_execution_local_only",
    "local_vortex_primitive_execution_count_all_decode_required",
    "local_vortex_primitive_execution_filter_project_decode_boundary_reported",
    "local_vortex_primitive_execution_scan_filter_pushdown",
    "local_vortex_primitive_execution_scan_projection_pushdown",
    "local_vortex_primitive_execution_row_read",
    "local_vortex_primitive_execution_arrow_converted",
    "local_vortex_primitive_execution_object_store_io",
    "local_vortex_primitive_execution_write_io",
    "local_vortex_primitive_execution_spill_io",
    "local_vortex_primitive_execution_requires_correctness_evidence",
    "local_vortex_primitive_execution_requires_benchmark_for_production",
    "local_vortex_primitive_execution_fallback_execution_allowed",
];

const ADAPTER_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "adapter_entry_count",
    "read_supported_count",
];

const SEMANTIC_PROFILE_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "semantic_profile_count",
    "dimensions_declared_count",
];

const MIGRATION_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "migration_report_count",
    "supported_construct_count",
];

const CERTIFICATION_FIELD_KEYS: [&str; 46] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "sql_feature_count",
    "operator_family_count",
    "function_group_count",
    "adapter_entry_count",
    "best_choice_claim",
    "best_default_certification_gate_schema_version",
    "best_default_certification_gate_report_id",
    "best_default_certification_gate_docs_ref",
    "best_default_certification_gate_source_refs",
    "best_default_certification_gate_support_status",
    "best_default_certification_gate_status",
    "best_default_certification_gate_claim_gate_status",
    "best_default_certification_gate_required_evidence",
    "best_default_certification_gate_missing_evidence",
    "best_default_certification_gate_attached_evidence_refs",
    "best_default_certification_gate_blocker_ids",
    "best_default_certification_gate_correctness_evidence_required",
    "best_default_certification_gate_benchmark_evidence_required",
    "best_default_certification_gate_execution_certificate_required",
    "best_default_certification_gate_native_io_certificate_required",
    "best_default_certification_gate_materialization_decode_required",
    "best_default_certification_gate_no_fallback_policy_required",
    "best_default_certification_gate_release_security_required",
    "best_default_certification_gate_ux_install_docs_required",
    "best_default_certification_gate_all_required_evidence_attached",
    "best_default_language_allowed",
    "best_default_certification_gate_best_default_claim_allowed",
    "best_default_certification_gate_performance_claim_allowed",
    "best_default_certification_gate_superiority_claim_allowed",
    "best_default_certification_gate_spark_replacement_claim_allowed",
    "best_default_certification_gate_production_claim_allowed",
    "best_default_certification_gate_runtime_execution",
    "best_default_certification_gate_fallback_attempted",
    "best_default_certification_gate_external_engine_invoked",
    "best_default_certification_gate_claim_boundary",
];

const WORLD_CLASS_SURFACE_FIELD_KEYS: [&str; 25] = [
    "scope",
    "schema_version",
    "support_status",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "dimension",
    "dimension_status",
    "required",
    "correctness_evidence_required",
    "semantic_conformance_required",
    "benchmark_evidence_required",
    "adapter_certification_required",
    "native_io_certificate_required",
    "execution_certificate_required",
    "capability_snapshot_required",
    "surface_components",
    "production_claim_allowed",
    "best_default_publication_allowed",
];

const DATAFRAME_WORLD_CLASS_SURFACE_FIELD_KEYS: [&str; 46] = [
    "scope",
    "schema_version",
    "support_status",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "dimension",
    "dimension_status",
    "required",
    "correctness_evidence_required",
    "semantic_conformance_required",
    "benchmark_evidence_required",
    "adapter_certification_required",
    "native_io_certificate_required",
    "execution_certificate_required",
    "capability_snapshot_required",
    "surface_components",
    "production_claim_allowed",
    "best_default_publication_allowed",
    "planner_readiness_schema_version",
    "planner_readiness_matrix_id",
    "planner_readiness_report_ref",
    "planner_readiness_docs_ref",
    "planner_readiness_support_status_vocabulary",
    "planner_readiness_claim_gate_status",
    "planner_readiness_row_count",
    "planner_readiness_row_order",
    "planner_readiness_sql_row_order",
    "planner_readiness_dataframe_row_order",
    "planner_readiness_unsupported_diagnostic_codes",
    "planner_readiness_blocker_ids",
    "planner_readiness_required_evidence",
    "planner_readiness_parser_executed",
    "planner_readiness_binder_executed",
    "planner_readiness_planner_executed",
    "planner_readiness_runtime_execution",
    "planner_readiness_dataframe_runtime",
    "planner_readiness_external_engine_invoked",
    "planner_readiness_fallback_attempted",
    "planner_readiness_deterministic_diagnostics_present",
];

const WORLD_CLASS_SURFACE_SCOPES: [&str; 13] = [
    "data-etl",
    "python",
    "dataframe",
    "notebook",
    "udfs",
    "universal-adapters",
    "event-api-saas-adapters",
    "unstructured-media",
    "api-surfaces",
    "observability",
    "deployment",
    "extensions",
    "security-governance",
];

#[test]
fn capability_discovery_json_field_keys_are_stable() {
    for (scope, expected_keys) in [
        ("functions", FUNCTION_FIELD_KEYS.as_slice()),
        ("operators", OPERATOR_FIELD_KEYS.as_slice()),
        ("adapters", ADAPTER_FIELD_KEYS.as_slice()),
        ("semantic-profiles", SEMANTIC_PROFILE_FIELD_KEYS.as_slice()),
        ("migration", MIGRATION_FIELD_KEYS.as_slice()),
        ("certification", CERTIFICATION_FIELD_KEYS.as_slice()),
        ("engines", ENGINE_FIELD_KEYS.as_slice()),
        ("workflow", WORKFLOW_FIELD_KEYS.as_slice()),
        ("remote-api", REMOTE_API_FIELD_KEYS.as_slice()),
        ("cross-cg", CROSS_CG_FIELD_KEYS.as_slice()),
    ] {
        let output = run_capabilities_scope(scope);
        let keys = field_keys(&output);
        assert_eq!(keys.as_slice(), expected_keys, "scope={scope}");
    }

    let output = run_capabilities_scope("sql");
    let keys = field_keys(&output);
    let keys: Vec<String> = keys.into_iter().map(str::to_string).collect();
    assert_eq!(
        keys.as_slice(),
        with_sql_generated_source_alignment_fields(SQL_FIELD_KEYS.as_slice()).as_slice(),
        "scope=sql"
    );

    for scope in WORLD_CLASS_SURFACE_SCOPES {
        let output = run_capabilities_scope(scope);
        let keys = field_keys(&output);
        let keys: Vec<String> = keys.into_iter().map(str::to_string).collect();
        let expected_keys = match scope {
            "python" => with_dataframe_notebook_package_and_generated_source_alignment_fields(
                WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice(),
            ),
            "api-surfaces" => {
                with_dataframe_notebook_package_command_registry_wrapper_and_unstructured_fields(
                    WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice(),
                )
            }
            "universal-adapters" => {
                let mut keys: Vec<String> =
                    with_generated_source_fields(WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice())
                        .into_iter()
                        .map(str::to_string)
                        .collect();
                append_external_effect_and_admission_keys(&mut keys);
                append_unstructured_adapter_capability_keys(&mut keys);
                keys
            }
            "dataframe" => with_dataframe_notebook_package_and_generated_source_alignment_fields(
                DATAFRAME_WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice(),
            ),
            "notebook" | "deployment" => with_dataframe_notebook_package_readiness_fields(
                WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice(),
            ),
            "observability" => {
                with_observability_contract_fields(WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice())
            }
            "event-api-saas-adapters" | "unstructured-media" => {
                with_external_effect_and_unstructured_adapter_fields(
                    WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice(),
                )
            }
            "security-governance" => {
                with_security_governance_policy_fields(WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice())
            }
            "udfs" | "extensions" => {
                with_extension_manifest_effect_fields(WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice())
            }
            _ => WORLD_CLASS_SURFACE_FIELD_KEYS
                .into_iter()
                .map(str::to_string)
                .collect(),
        };
        assert_eq!(keys.as_slice(), expected_keys.as_slice(), "scope={scope}");
    }
}

#[test]
fn capability_discovery_json_fields_remain_report_only() {
    for scope in [
        "sql",
        "functions",
        "operators",
        "adapters",
        "semantic-profiles",
        "migration",
        "certification",
        "data-etl",
        "python",
        "dataframe",
        "notebook",
        "udfs",
        "universal-adapters",
        "event-api-saas-adapters",
        "unstructured-media",
        "api-surfaces",
        "observability",
        "deployment",
        "extensions",
        "security-governance",
        "engines",
        "workflow",
        "remote-api",
        "cross-cg",
        "compatibility",
    ] {
        let output = run_capabilities_scope(scope);
        for key in REPORT_ONLY_BOOL_FIELD_KEYS {
            let expected_value = key == "side_effect_free";
            assert!(
                output.contains(&field_pair(key, expected_value)),
                "scope={scope} key={key}"
            );
        }
        assert!(output.contains("\"attempted\":false"), "scope={scope}");
        assert!(output.contains("\"allowed\":false"), "scope={scope}");
        assert!(output.contains("\"diagnostics\":[]"), "scope={scope}");
        assert!(!output.contains("generated_at"), "scope={scope}");
    }
}

#[test]
fn capability_discovery_scope_values_are_stable() {
    for (scope, field_value) in [
        ("sql", "sql"),
        ("functions", "functions"),
        ("operators", "operators"),
        ("adapters", "adapters"),
        ("semantic-profiles", "semantic_profiles"),
        ("migration", "migration"),
        ("certification", "certification"),
        ("data-etl", "data_etl"),
        ("python", "python"),
        ("dataframe", "dataframe"),
        ("notebook", "notebook"),
        ("udfs", "udfs"),
        ("universal-adapters", "universal_adapters"),
        ("event-api-saas-adapters", "event_api_saas_adapters"),
        ("unstructured-media", "unstructured_media"),
        ("api-surfaces", "api_surfaces"),
        ("observability", "observability"),
        ("deployment", "deployment"),
        ("extensions", "extensions"),
        ("security-governance", "security_governance"),
        ("engines", "engines"),
        ("workflow", "workflow"),
        ("remote-api", "remote_api"),
        ("cross-cg", "cross_cg"),
        ("compatibility", "compatibility"),
    ] {
        let output = run_capabilities_scope(scope);
        assert!(
            output.contains(&format!(
                "{{\"key\":\"scope\",\"value\":\"{field_value}\"}}"
            )),
            "scope={scope}"
        );
    }
}

#[test]
fn udf_and_effectful_capabilities_expose_external_effect_blockers() {
    for scope in [
        "udfs",
        "event-api-saas-adapters",
        "universal-adapters",
        "unstructured-media",
        "extensions",
        "security-governance",
    ] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "external_effect_blocker_matrix_schema_version",
            "shardloom.external_effect_blocker_matrix.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "external_effect_blocker_matrix_id",
            "gar-0032-c.udf_external_effect_blockers"
        )));
        assert!(output.contains(&string_field_pair(
            "external_effect_blocker_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&field_pair(
            "external_effect_blocker_all_effects_blocked",
            true
        )));
        assert!(output.contains(&field_pair(
            "external_effect_blocker_runtime_execution",
            false
        )));
        assert!(output.contains(&field_pair(
            "external_effect_blocker_network_probe_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "external_effect_blocker_fallback_attempted",
            false
        )));
        assert!(output.contains(&field_pair(
            "external_effect_blocker_external_engine_invoked",
            false
        )));
        assert!(output.contains(&string_field_pair(
            "effectful_operation_admission_matrix_schema_version",
            "shardloom.effectful_operation_admission_matrix.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "effectful_operation_admission_claim_gate_status",
            "fixture_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "effectful_operation_admission_row_local_sqlite_import_export_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&string_field_pair(
            "effectful_operation_admission_row_deterministic_scalar_udf_fixture_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&string_field_pair(
            "effectful_operation_admission_row_network_database_connectors_support_status",
            "blocked"
        )));
        assert!(output.contains(&field_pair(
            "effectful_operation_admission_all_external_and_sandboxed_paths_blocked",
            true
        )));
        for row in [
            "sql_udf",
            "python_udf",
            "external_service_udf",
            "api_call",
            "llm_call",
            "embedding_generation",
            "plugin_execution",
            "media_extraction",
            "network_egress",
        ] {
            assert!(output.contains(&string_field_pair(
                &format!("external_effect_blocker_row_{row}_support_status"),
                "blocked"
            )));
            assert!(output.contains(&field_pair(
                &format!("external_effect_blocker_row_{row}_runtime_execution"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("external_effect_blocker_row_{row}_effect_executed"),
                false
            )));
        }
    }
}

#[test]
fn extension_capabilities_expose_manifest_effect_matrix() {
    for scope in ["udfs", "extensions", "security-governance"] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "extension_manifest_effect_matrix_schema_version",
            "shardloom.extension_manifest_effect_capability_matrix.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "extension_manifest_effect_matrix_id",
            "gar-0011-a.extension_manifest_external_effect_capability_matrix"
        )));
        assert!(output.contains(&string_field_pair(
            "extension_manifest_effect_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_all_runtime_blocked",
            true
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_all_external_effects_blocked",
            true
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_runtime_execution",
            false
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_extension_code_executed",
            false
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_dynamic_loading",
            false
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_fallback_attempted",
            false
        )));
        assert!(output.contains(&field_pair(
            "extension_manifest_effect_external_engine_invoked",
            false
        )));
        for row in [
            "metadata_only_manifest",
            "python_udf_extension",
            "object_store_provider_extension",
            "api_llm_effect_provider",
            "embedding_vector_provider",
        ] {
            assert!(output.contains(&field_pair(
                &format!("extension_manifest_effect_row_{row}_runtime_execution"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("extension_manifest_effect_row_{row}_extension_code_executed"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("extension_manifest_effect_row_{row}_external_effect_executed"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("extension_manifest_effect_row_{row}_fallback_attempted"),
                false
            )));
        }
    }
}

#[test]
fn extension_and_udf_capabilities_expose_plugin_abi_udf_sandbox_blocker() {
    for scope in ["udfs", "extensions", "security-governance"] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "plugin_abi_udf_sandbox_blocker_schema_version",
            "shardloom.plugin_abi_udf_sandbox_blocker.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "plugin_abi_udf_sandbox_blocker_id",
            "gar-0023-a.plugin_abi_udf_sandbox_blocker"
        )));
        assert!(output.contains(&string_field_pair(
            "plugin_abi_udf_sandbox_blocker_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
            true
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_abi_loading_supported",
            false
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_dynamic_loading_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_extension_code_executed",
            false
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_udf_execution_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
            false
        )));
        for row in [
            "dynamic_library_loading",
            "rust_native_udf",
            "wasm_udf",
            "python_udf",
            "sandbox_evidence_binding",
        ] {
            assert!(output.contains(&string_field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_support_status"),
                "blocked"
            )));
            assert!(output.contains(&field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_runtime_execution"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_extension_code_executed"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_udf_execution_performed"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_fallback_attempted"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_external_engine_invoked"),
                false
            )));
        }
    }
}

#[test]
fn security_governance_capabilities_expose_credential_policy_gate() {
    let output = run_capabilities_scope("security-governance");
    assert!(output.contains(&string_field_pair(
        "credential_policy_gate_schema_version",
        "shardloom.credential_policy_enforcement_gate.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "credential_policy_gate_id",
        "gar-0019-a.credential_lifecycle_policy_enforcement_gate"
    )));
    assert!(output.contains(&string_field_pair(
        "credential_policy_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_all_credential_runtime_blocked",
        true
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_credential_references_only",
        true
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_credential_resolution_performed",
        false
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_secret_loading_performed",
        false
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_secret_value_materialized",
        false
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "credential_policy_gate_external_engine_invoked",
        false
    )));
    for row in [
        "secret_loading",
        "environment_secret_provider",
        "external_secret_manager_provider",
        "cloud_iam_provider",
        "runtime_permission_check",
    ] {
        assert!(output.contains(&field_pair(
            &format!("credential_policy_gate_row_{row}_credential_resolution_performed"),
            false
        )));
        assert!(output.contains(&field_pair(
            &format!("credential_policy_gate_row_{row}_secret_loading_performed"),
            false
        )));
        assert!(output.contains(&field_pair(
            &format!("credential_policy_gate_row_{row}_fallback_attempted"),
            false
        )));
    }
}

#[test]
fn security_governance_capabilities_expose_sandbox_governance_gate() {
    let output = run_capabilities_scope("security-governance");
    assert!(output.contains(&string_field_pair(
        "sandbox_governance_gate_schema_version",
        "shardloom.sandbox_governance_readiness_gate.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "sandbox_governance_gate_id",
        "gar-0019-b.sandbox_governance_runtime_readiness"
    )));
    assert!(output.contains(&string_field_pair(
        "sandbox_governance_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_all_sandbox_runtime_blocked",
        true
    )));
    assert!(output.contains(&field_pair("sandbox_governance_gate_deny_by_default", true)));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_sandbox_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_extension_code_executed",
        false
    )));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_udf_code_executed",
        false
    )));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "sandbox_governance_gate_external_engine_invoked",
        false
    )));
    for row in [
        "filesystem_permission",
        "network_permission",
        "environment_access",
        "process_execution",
        "resource_limits",
    ] {
        assert!(output.contains(&field_pair(
            &format!("sandbox_governance_gate_row_{row}_sandbox_enforced"),
            false
        )));
        assert!(output.contains(&field_pair(
            &format!("sandbox_governance_gate_row_{row}_external_effect_executed"),
            false
        )));
        assert!(output.contains(&field_pair(
            &format!("sandbox_governance_gate_row_{row}_fallback_attempted"),
            false
        )));
    }
}

#[test]
fn unstructured_and_adapter_capabilities_expose_report_only_matrix() {
    for scope in [
        "unstructured-media",
        "universal-adapters",
        "event-api-saas-adapters",
        "api-surfaces",
    ] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "unstructured_adapter_capability_schema_version",
            "shardloom.unstructured_adapter_capability_matrix.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "unstructured_adapter_capability_matrix_id",
            "gar-0032-d.unstructured_media_universal_adapter_matrix"
        )));
        assert!(output.contains(&string_field_pair(
            "unstructured_adapter_capability_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_runtime_execution",
            false
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_source_io_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_sink_io_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_model_call_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_network_probe_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "unstructured_adapter_capability_external_engine_invoked",
            false
        )));
        for row in [
            "document_reference",
            "text_extraction",
            "image_audio_video",
            "embedding_vector_generation",
            "vector_search",
            "vortex_turboquant_vector_encoding",
            "universal_file_adapter",
            "database_warehouse_adapter",
            "object_store_table_adapter",
            "event_api_saas_adapter",
            "source_sink_metadata",
        ] {
            assert!(output.contains(&format!(
                "{{\"key\":\"unstructured_adapter_capability_row_{row}_support_status\",\"value\":"
            )));
            assert!(output.contains(&field_pair(
                &format!("unstructured_adapter_capability_row_{row}_runtime_execution"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("unstructured_adapter_capability_row_{row}_source_io_performed"),
                false
            )));
            assert!(output.contains(&field_pair(
                &format!("unstructured_adapter_capability_row_{row}_sink_io_performed"),
                false
            )));
        }
    }
}

#[test]
fn dataframe_notebook_package_readiness_distinguishes_install_from_runtime_support() {
    for scope in [
        "python",
        "dataframe",
        "notebook",
        "deployment",
        "api-surfaces",
    ] {
        let output = run_capabilities_scope(scope);
        assert_dataframe_notebook_package_readiness_summary(&output);
        assert_dataframe_notebook_package_readiness_rows(&output);
    }
}

fn assert_dataframe_notebook_package_readiness_summary(output: &str) {
    assert!(output.contains(&string_field_pair(
        "dataframe_notebook_package_readiness_schema_version",
        "shardloom.dataframe_notebook_package_readiness.v1",
    )));
    assert!(output.contains(&string_field_pair(
        "dataframe_notebook_package_readiness_report_id",
        "gar-0010-b.dataframe_notebook_package_readiness",
    )));
    assert!(output.contains(&string_field_pair(
        "dataframe_notebook_package_readiness_claim_gate_status",
        "not_claim_grade",
    )));
    for (key, value) in [
        (
            "dataframe_notebook_package_readiness_local_install_smoke_supported",
            true,
        ),
        (
            "dataframe_notebook_package_readiness_installed_package_smoke_distinct_from_runtime_support",
            true,
        ),
        (
            "dataframe_notebook_package_readiness_dataframe_runtime_supported",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_notebook_runtime_supported",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_package_publication_ready",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_package_publication_claim_allowed",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_fallback_attempted",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_external_engine_invoked",
            false,
        ),
        (
            "dataframe_notebook_package_readiness_all_rows_no_runtime_claims",
            true,
        ),
    ] {
        assert!(output.contains(&field_pair(key, value)));
    }
}

fn assert_dataframe_notebook_package_readiness_rows(output: &str) {
    assert!(output.contains(&string_field_pair(
        "dataframe_notebook_package_readiness_row_order",
        "python_package_metadata,editable_install_smoke,dataframe_method_matrix,notebook_display_surface,public_package_publication,unsupported_diagnostics",
    )));
    for (key, value) in [
        (
            "dataframe_notebook_package_readiness_row_editable_install_smoke_support_status",
            "smoke_supported",
        ),
        (
            "dataframe_notebook_package_readiness_row_dataframe_method_matrix_support_status",
            "report_only",
        ),
        (
            "dataframe_notebook_package_readiness_row_notebook_display_surface_support_status",
            "blocked",
        ),
        (
            "dataframe_notebook_package_readiness_row_public_package_publication_blocker_id",
            "gar-0024.package_publication_gate_required",
        ),
    ] {
        assert!(output.contains(&string_field_pair(key, value)));
    }
    for row in DATAFRAME_NOTEBOOK_PACKAGE_READINESS_ROW_IDS {
        for suffix in [
            "package_publication_allowed",
            "dataframe_runtime_supported",
            "notebook_runtime_supported",
            "fallback_attempted",
            "external_engine_invoked",
        ] {
            assert!(output.contains(&field_pair(
                &format!("dataframe_notebook_package_readiness_row_{row}_{suffix}"),
                false,
            )));
        }
    }
}

#[test]
fn compatibility_capabilities_expose_universal_scoreboard() {
    let output = run_capabilities_scope("compatibility");

    assert_universal_compatibility_top_level_keys(&output);
    assert_universal_compatibility_source_rows(&output);
    assert_universal_compatibility_unsupported_boundaries(&output);
    assert_generated_output_compatibility_fields(&output);
    assert_object_store_ladder_fields(&output);
    assert_table_format_matrix_fields(&output);
    assert_database_warehouse_matrix_fields(&output);
}

fn assert_universal_compatibility_top_level_keys(output: &str) {
    for key in [
        "universal_compatibility_scoreboard_schema_version",
        "universal_compatibility_scoreboard_id",
        "universal_compatibility_scoreboard_docs_ref",
        "universal_compatibility_scoreboard_data_ref",
        "universal_compatibility_support_status_vocabulary",
        "universal_compatibility_row_count",
        "universal_compatibility_row_order",
        "universal_compatibility_claim_boundary",
        "universal_compatibility_generated_output_contract_schema_version",
        "universal_compatibility_generated_output_contract_id",
        "universal_compatibility_generated_output_row_order",
        "universal_compatibility_generated_output_python_row_order",
        "universal_compatibility_generated_output_sql_row_order",
        "universal_compatibility_generated_output_dataframe_row_order",
        "universal_compatibility_object_store_ladder_schema_version",
        "universal_compatibility_object_store_ladder_id",
        "universal_compatibility_object_store_ladder_row_order",
        "universal_compatibility_table_format_matrix_schema_version",
        "universal_compatibility_table_format_matrix_id",
        "universal_compatibility_table_format_matrix_row_order",
        "universal_compatibility_database_warehouse_matrix_schema_version",
        "universal_compatibility_database_warehouse_matrix_id",
        "universal_compatibility_database_warehouse_matrix_row_order",
    ] {
        assert!(
            output.contains(&format!("{{\"key\":\"{key}\",\"value\":")),
            "missing universal compatibility key {key}"
        );
    }
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_scoreboard_schema_version",
        "shardloom.universal_compatibility_coverage_scoreboard.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_scoreboard_data_ref",
        "docs/architecture/universal-compatibility-coverage-scoreboard.json"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_support_status_vocabulary",
        "runtime-supported,smoke-supported,report-only,blocked,not-planned"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_order",
        "csv,jsonl_ndjson,json,parquet,arrow_ipc,avro,orc,excel,sqlite,postgres_mysql,jdbc_odbc,object_store_s3_gcs_adls,table_lakehouse_iceberg_delta_hudi,vortex,generated_source_free_outputs,python_rows_dataframe,sql_values_literals,rest_flight_adbc,foundry"
    )));
}

fn assert_universal_compatibility_source_rows(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_csv_support_status",
        "runtime-supported"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_jsonl_ndjson_support_status",
        "runtime-supported"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_json_support_status",
        "runtime-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_row_json_output_io_performed",
        false
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_sqlite_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_row_sqlite_source_io_performed",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_row_sqlite_output_io_performed",
        true
    )));
}

fn assert_universal_compatibility_unsupported_boundaries(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_object_store_s3_gcs_adls_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_row_object_store_s3_gcs_adls_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_row_object_store_s3_gcs_adls_external_engine_invoked",
        false
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_row_vortex_support_status",
        "runtime-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_sql_dataframe_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_foundry_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_all_rows_fallback_attempted_false",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_all_rows_external_engine_invoked_false",
        true
    )));
}

#[test]
fn runs_today_exposes_generated_current_support_matrix() {
    let output = run_runs_today();

    assert_runs_today_summary_fields(&output);
    assert_runs_today_effect_fields(&output);
    assert_runs_today_row_states(&output);
    assert_runs_today_evidence_refs(&output);
}

fn assert_runs_today_summary_fields(output: &str) {
    for (key, value) in [
        (
            "runs_today_schema_version",
            "shardloom.runs_today_support_matrix.v1",
        ),
        ("runs_today_matrix_id", "review-p0-1.current-support"),
        (
            "runs_today_support_state_vocabulary",
            "executable,feature_gated,diagnostic_only,report_only,blocked,future",
        ),
        (
            "runs_today_family_order",
            "cli_command,python_api,input_format,output_format,execution_mode,claim_state",
        ),
        ("runs_today_row_count", "33"),
        ("runs_today_executable_row_count", "18"),
        ("runs_today_feature_gated_row_count", "5"),
        ("runs_today_diagnostic_only_row_count", "3"),
        ("runs_today_report_only_row_count", "1"),
        ("runs_today_blocked_row_count", "5"),
        ("runs_today_future_row_count", "1"),
        ("runs_today_cli_command_row_count", "8"),
        ("runs_today_python_api_row_count", "5"),
        ("runs_today_input_format_row_count", "6"),
        ("runs_today_output_format_row_count", "3"),
        ("runs_today_execution_mode_row_count", "6"),
        ("runs_today_claim_state_row_count", "5"),
    ] {
        assert!(
            output.contains(&string_field_pair(key, value)),
            "missing runs-today field {key}={value}"
        );
    }
}

fn assert_runs_today_effect_fields(output: &str) {
    for key in [
        "fallback_execution_allowed",
        "fallback_attempted",
        "external_engine_invoked",
        "runs_today_runtime_expansion_allowed",
        "runs_today_package_publication_allowed",
        "runs_today_performance_claim_allowed",
    ] {
        assert!(
            output.contains(&field_pair(key, false)),
            "missing false {key}"
        );
    }
    for key in [
        "side_effect_free",
        "runtime_discovery_side_effect_free",
        "runs_today_all_rows_fallback_attempted_false",
        "runs_today_all_rows_external_engine_invoked_false",
        "runs_today_all_rows_no_fallback_no_external_engine",
    ] {
        assert!(
            output.contains(&field_pair(key, true)),
            "missing true {key}"
        );
    }
}

fn assert_runs_today_row_states(output: &str) {
    for (row, support_state) in [
        ("cli_sql_local_source_smoke", "executable"),
        ("cli_vortex_ingest_smoke", "feature_gated"),
        (
            "cli_direct_transient_local_adapter_benchmark",
            "feature_gated",
        ),
        ("cli_sqlite_local_import_export_smoke", "executable"),
        ("cli_udf_local_scalar_fixture_smoke", "executable"),
        ("python_status_capabilities", "diagnostic_only"),
        ("python_effectful_fixture_helpers", "executable"),
        ("input_sqlite_local_database_file", "executable"),
        ("input_object_store_cloud", "blocked"),
        ("input_object_store_public_fixture", "executable"),
        ("execution_report_only_surfaces", "report_only"),
        ("execution_live_hybrid_remote_distributed", "future"),
        ("claim_performance_superiority", "blocked"),
    ] {
        assert!(output.contains(&string_field_pair(
            &format!("runs_today_row_{row}_support_state"),
            support_state
        )));
        assert!(output.contains(&field_pair(
            &format!("runs_today_row_{row}_fallback_attempted"),
            false
        )));
        assert!(output.contains(&field_pair(
            &format!("runs_today_row_{row}_external_engine_invoked"),
            false
        )));
    }
}

fn assert_runs_today_evidence_refs(output: &str) {
    assert!(output.contains(&string_field_pair(
        "runs_today_row_cli_sql_local_source_smoke_evidence_refs",
        "sql_local_source_runtime_smoke,sql_frontend_runtime_ladder_fields,sql_parser_tests,python_query_builder_tests"
    )));
    assert!(output.contains(&string_field_pair(
        "runs_today_row_cli_vortex_ingest_smoke_evidence_refs",
        "sql_local_source_runtime_smoke,vortex_ingest_evidence_fields,vortex_preparation_spine_evidence_fields,vortex_scout_ingress_evidence_fields,vortex_layout_write_advisor_evidence_fields,vortex_copy_budget_evidence_fields,vortex_differential_preparation_evidence_fields,vortex_capillary_preparation_evidence_fields"
    )));
    assert!(output.contains(&string_field_pair(
        "runs_today_row_input_parquet_arrow_avro_orc_evidence_refs",
        "feature_gated_sql_local_source_tests,vortex_ingest_smoke_structured_adapter_tests,vortex_preparation_spine_evidence_fields,vortex_scout_ingress_evidence_fields,vortex_layout_write_advisor_evidence_fields,vortex_copy_budget_evidence_fields,vortex_differential_preparation_evidence_fields,vortex_capillary_preparation_evidence_fields,traditional_direct_transient_structured_tests,universal_ingress_route_taxonomy"
    )));
}

fn assert_generated_output_compatibility_fields(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_contract_schema_version",
        "shardloom.universal_compatibility.generated_output_contract.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_order",
        "no_dataset_smoke,python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write,local_output_only_generated_source_posture,sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range,dataframe_source_free_projection,dataframe_generated_with_column"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_python_ctx_from_rows_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_python_ctx_from_rows_generated_source_created",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_python_ctx_sequence_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_sql_literal_select_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_sql_literal_select_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_sql_values_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_sql_values_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_sql_source_free_projection_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_sql_source_free_projection_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_sql_generate_series_range_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_sql_generate_series_range_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_dataframe_generated_with_column_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_row_dataframe_generated_with_column_runtime_execution",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_generated_output_row_local_output_only_generated_source_posture_blocker_id",
        "gar-compat-1b.non_local_generated_output_blocked"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_no_dataset_smoke_separate",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_object_store_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_foundry_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed",
        false
    )));
}

fn assert_object_store_ladder_fields(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_schema_version",
        "shardloom.universal_compatibility.object_store_admission_ladder.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_row_order",
        "object_store_uri_parse,credential_policy,public_no_credential_read,authenticated_read,byte_range_read,full_file_read,local_cache,write_staging,commit_protocol"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_row_object_store_uri_parse_support_status",
        "report-only"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_row_public_no_credential_read_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_row_byte_range_read_blocker_id",
        "gar-compat-1c.byte_range_read_runtime_blocked"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_object_store_ladder_row_authenticated_read_credential_policy_status",
        "authenticated_read_policy_required"
    )));
    for key in [
        "universal_compatibility_object_store_ladder_authenticated_read_supported",
        "universal_compatibility_object_store_ladder_byte_range_read_supported",
        "universal_compatibility_object_store_ladder_full_file_read_supported",
        "universal_compatibility_object_store_ladder_write_staging_supported",
        "universal_compatibility_object_store_ladder_commit_protocol_supported",
        "universal_compatibility_object_store_ladder_credential_resolution_performed",
        "universal_compatibility_object_store_ladder_network_probe_allowed",
        "universal_compatibility_object_store_ladder_provider_probe_allowed",
        "universal_compatibility_object_store_ladder_write_io",
        "universal_compatibility_object_store_ladder_fallback_attempted",
        "universal_compatibility_object_store_ladder_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field_pair(key, false)),
            "missing false key={key}"
        );
    }
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_runtime_supported",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_public_no_credential_read_supported",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_object_store_io",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_all_rows_no_effects",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_all_live_provider_effects_disabled",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_object_store_ladder_all_rows_no_fallback_no_external_engine",
        true
    )));
}

fn assert_table_format_matrix_fields(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_table_format_matrix_schema_version",
        "shardloom.universal_compatibility.table_format_boundary_matrix.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_table_format_matrix_row_order",
        "table_metadata_read,table_scan,snapshot_time_travel,partition_evolution,delete_tombstone,append,merge_update_delete,commit,rollback,catalog_interaction,object_store_coupling"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_table_format_matrix_row_table_metadata_read_support_status",
        "report-only"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_table_format_matrix_row_table_scan_blocker_id",
        "gar-compat-1d.table_scan_runtime_blocked"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_table_format_matrix_row_commit_blocker_id",
        "gar-compat-1d.table_commit_blocked"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_table_format_matrix_local_metadata_smoke_available",
        true
    )));
    for key in [
        "universal_compatibility_table_format_matrix_runtime_supported",
        "universal_compatibility_table_format_matrix_table_metadata_read_supported",
        "universal_compatibility_table_format_matrix_table_scan_supported",
        "universal_compatibility_table_format_matrix_table_write_supported",
        "universal_compatibility_table_format_matrix_table_commit_supported",
        "universal_compatibility_table_format_matrix_table_rollback_supported",
        "universal_compatibility_table_format_matrix_catalog_interaction_supported",
        "universal_compatibility_table_format_matrix_object_store_runtime_supported",
        "universal_compatibility_table_format_matrix_external_table_format_dependency_added",
        "universal_compatibility_table_format_matrix_fallback_attempted",
        "universal_compatibility_table_format_matrix_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field_pair(key, false)),
            "missing false key={key}"
        );
    }
    assert!(output.contains(&field_pair(
        "universal_compatibility_table_format_matrix_all_rows_no_io_no_fallback",
        true
    )));
}

fn assert_database_warehouse_matrix_fields(output: &str) {
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_database_warehouse_matrix_schema_version",
        "shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_database_warehouse_matrix_row_order",
        "sqlite_file,postgres,mysql,jdbc_odbc,snowflake,bigquery,databricks_sql"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_database_warehouse_matrix_row_sqlite_file_support_status",
        "smoke-supported"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_runtime_supported",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_import_runtime_supported",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_export_runtime_supported",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_database_warehouse_matrix_row_postgres_blocker_id",
        "gar-compat-1e.postgres_connector_runtime_blocked"
    )));
    assert!(output.contains(&string_field_pair(
        "universal_compatibility_database_warehouse_matrix_row_jdbc_odbc_blocker_id",
        "gar-compat-1e.jdbc_odbc_driver_loading_blocked"
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_external_baseline_only",
        false
    )));
    for key in [
        "universal_compatibility_database_warehouse_matrix_query_pushdown_supported",
        "universal_compatibility_database_warehouse_matrix_credential_resolution_performed",
        "universal_compatibility_database_warehouse_matrix_network_probe_performed",
        "universal_compatibility_database_warehouse_matrix_driver_loaded",
        "universal_compatibility_database_warehouse_matrix_fallback_attempted",
        "universal_compatibility_database_warehouse_matrix_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field_pair(key, false)),
            "missing false key={key}"
        );
    }
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_all_rows_no_effects",
        true
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_row_bigquery_query_pushdown_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "universal_compatibility_database_warehouse_matrix_row_databricks_sql_external_baseline_only",
        true
    )));
}

#[test]
fn sql_and_dataframe_capabilities_expose_planner_readiness_matrix() {
    for scope in ["sql", "dataframe"] {
        let output = run_capabilities_scope(scope);
        assert_planner_readiness_fields(&output, scope);
        if scope == "sql" {
            assert_sql_frontend_runtime_ladder_fields(&output);
            assert_sql_local_source_smoke_fields(&output);
        } else {
            assert!(!output.contains("sql_frontend_runtime_ladder_schema_version"));
            assert!(!output.contains("sql_local_source_smoke_schema_version"));
        }
    }
}

fn assert_planner_readiness_fields(output: &str, scope: &str) {
    for key in PLANNER_READINESS_FIELD_KEYS {
        assert!(
            output.contains(&format!("{{\"key\":\"{key}\",\"value\":")),
            "scope={scope} missing key={key}"
        );
    }
    for (key, value) in [
        (
            "planner_readiness_schema_version",
            "shardloom.sql_dataframe_planner_readiness.v1",
        ),
        ("planner_readiness_claim_gate_status", "not_claim_grade"),
        (
            "planner_readiness_sql_row_order",
            "sql_text_admission,sql_parse,sql_bind,sql_plan,sql_execute",
        ),
        (
            "planner_readiness_dataframe_row_order",
            "dataframe_lazy_plan,dataframe_expression_builder,dataframe_join,dataframe_aggregate,dataframe_window",
        ),
        (
            "planner_readiness_unsupported_diagnostic_codes",
            "SL_SQL_TEXT_ADMISSION_REPORT_ONLY,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_DATAFRAME_LAZY_PLAN_REPORT_ONLY,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_PLANNER_READINESS_DIAGNOSTICS_REPORT_ONLY,SL_UNSUPPORTED_PLANNER_EXECUTION_STATE",
        ),
    ] {
        assert!(output.contains(&string_field_pair(key, value)));
    }
    assert_planner_readiness_boolean_fields(output);
}

fn assert_planner_readiness_boolean_fields(output: &str) {
    for key in [
        "planner_readiness_parser_executed",
        "planner_readiness_binder_executed",
        "planner_readiness_planner_executed",
        "planner_readiness_runtime_execution",
        "planner_readiness_dataframe_runtime",
        "planner_readiness_external_engine_invoked",
        "planner_readiness_fallback_attempted",
    ] {
        assert!(output.contains(&field_pair(key, false)));
    }
    assert!(output.contains(&field_pair(
        "planner_readiness_deterministic_diagnostics_present",
        true
    )));
}

fn assert_sql_frontend_runtime_ladder_fields(output: &str) {
    for key in SQL_FRONTEND_RUNTIME_LADDER_FIELD_KEYS {
        assert!(
            output.contains(&format!("{{\"key\":\"{key}\",\"value\":")),
            "missing SQL frontend runtime ladder key={key}"
        );
    }
    for (key, value) in [
        (
            "sql_frontend_runtime_ladder_schema_version",
            "shardloom.sql_frontend_runtime_ladder.v1",
        ),
        (
            "sql_frontend_runtime_ladder_runtime_family_order",
            "local_source_projection_filter_limit,local_source_predicate_expression_ladder,local_source_aggregate_group_having,local_source_order_topn,local_source_join_ladder,local_source_window_ladder,local_source_output_fanout,source_free_sql_generated_output",
        ),
        (
            "sql_frontend_runtime_ladder_blocked_family_order",
            "broad_sql_parse_bind_plan_execute,catalog_cte_setop_recursive_sql,correlated_and_broad_subquery_sql,object_store_table_sql,fallback_engine_sql",
        ),
        (
            "sql_frontend_runtime_ladder_row_local_source_join_ladder_support_status",
            "smoke-supported",
        ),
        (
            "sql_frontend_runtime_ladder_row_broad_sql_parse_bind_plan_execute_blocker_id",
            "cg21.workflow.sql.execute_unsupported",
        ),
    ] {
        assert!(output.contains(&string_field_pair(key, value)));
    }
    assert_sql_frontend_runtime_ladder_boolean_fields(output);
}

fn assert_sql_frontend_runtime_ladder_boolean_fields(output: &str) {
    for (key, value) in [
        ("sql_frontend_runtime_ladder_parser_executed", true),
        ("sql_frontend_runtime_ladder_runtime_execution", true),
        ("sql_frontend_runtime_ladder_dataframe_runtime", false),
        ("sql_frontend_runtime_ladder_fallback_attempted", false),
        ("sql_frontend_runtime_ladder_external_engine_invoked", false),
        ("sql_frontend_runtime_ladder_broad_sql_claim_allowed", false),
        (
            "sql_frontend_runtime_ladder_row_local_source_window_ladder_runtime_execution",
            true,
        ),
        (
            "sql_frontend_runtime_ladder_row_fallback_engine_sql_external_engine_invoked",
            false,
        ),
    ] {
        assert!(output.contains(&field_pair(key, value)));
    }
}

fn assert_sql_local_source_smoke_fields(output: &str) {
    for (key, value) in [
        (
            "sql_local_source_smoke_schema_version",
            "shardloom.sql_local_source_smoke.v1",
        ),
        ("sql_local_source_smoke_command", "sql-local-source-smoke"),
        (
            "sql_local_source_smoke_support_status",
            "fixture_smoke_supported",
        ),
        (
            "sql_local_source_smoke_execution_mode",
            "direct_compatibility_transient",
        ),
        (
            "sql_local_source_smoke_claim_gate_status",
            "fixture_smoke_only",
        ),
    ] {
        assert!(output.contains(&string_field_pair(key, value)));
    }
    assert!(output.contains(&field_pair(
        "sql_local_source_smoke_runtime_execution",
        true
    )));
    assert!(output.contains(&field_pair(
        "sql_local_source_smoke_external_engine_invoked",
        false
    )));
    assert!(output.contains(&field_pair(
        "sql_local_source_smoke_fallback_attempted",
        false
    )));
}

#[test]
fn generated_source_capability_contract_separates_no_dataset_smoke() {
    for scope in [
        "sql",
        "python",
        "dataframe",
        "universal-adapters",
        "api-surfaces",
    ] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "generated_source_contract_schema_version",
            "shardloom.generated_source_certificate_contract.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_case_order",
            "no_dataset_smoke,user_generated_source,engine_native_generated_source"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_contract_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&field_pair(
            "generated_source_contract_fallback_attempted",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_contract_external_engine_invoked",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_contract_object_store_io_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_contract_foundry_runtime_invoked",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_contract_broad_sql_dataframe_claim_allowed",
            false
        )));
        assert!(output.contains(&string_field_pair(
            "no_dataset_smoke_support_status",
            "smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "no_dataset_smoke_generated_source_certificate_status",
            "not_applicable_no_generated_rows"
        )));
        assert!(output.contains(&field_pair(
            "no_dataset_smoke_generated_source_created",
            false
        )));
        assert!(output.contains(&field_pair("no_dataset_smoke_output_io_performed", false)));
        assert!(output.contains(&string_field_pair(
            "user_generated_source_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&string_field_pair(
            "user_generated_source_blocker_id",
            "none_scoped_local_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "user_generated_source_claim_gate_status",
            "fixture_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "engine_native_generated_source_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&string_field_pair(
            "engine_native_generated_source_blocker_id",
            "none_scoped_local_range_sequence_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&string_field_pair("input_dataset_count", "0")));
        assert!(output.contains(&field_pair("source_io_performed", false)));
        assert!(output.contains(&field_pair("generated_source_created", false)));
        assert!(output.contains(&field_pair("output_io_performed", false)));
        assert!(output.contains(&string_field_pair(
            "generated_source_certificate_status",
            "not_applicable_no_generated_rows"
        )));
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn generated_source_api_admission_matrix_classifies_source_free_surfaces() {
    for scope in ["sql", "python", "dataframe", "api-surfaces"] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_schema_version",
            "shardloom.generated_source_api_admission.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_matrix_id",
            "gar-gen-1e.source_free_api_admission"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_python_row_order",
            "python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_sql_row_order",
            "sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range"
        )));
        assert!(output.contains(&string_field_pair(
            "generated_source_api_admission_dataframe_row_order",
            "dataframe_source_free_projection,dataframe_generated_with_column"
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_data_read",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_source_io_performed",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_fallback_attempted",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_external_engine_invoked",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_fallback_execution_allowed",
            false
        )));
        assert!(output.contains(&field_pair(
            "generated_source_api_admission_broad_sql_dataframe_claim_allowed",
            false
        )));
        assert!(output.contains(&string_field_pair(
            "python_ctx_from_rows_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&field_pair("python_ctx_from_rows_runtime_execution", true)));
        assert!(output.contains(&field_pair("python_ctx_from_rows_write_io", true)));
        assert!(output.contains(&field_pair(
            "python_ctx_from_rows_source_io_performed",
            false
        )));
        assert!(output.contains(&string_field_pair(
            "python_ctx_range_blocker_id",
            "none_scoped_local_range_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "python_ctx_sequence_blocker_id",
            "none_scoped_local_sequence_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&field_pair("python_ctx_sequence_runtime_execution", true)));
        assert!(output.contains(&string_field_pair(
            "python_ctx_literal_table_blocker_id",
            "none_scoped_local_literal_table_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "python_ctx_calendar_support_status",
            "fixture_smoke_supported"
        )));
        assert!(output.contains(&field_pair("python_ctx_calendar_runtime_execution", true)));
        assert!(output.contains(&string_field_pair(
            "sql_values_blocker_id",
            "none_scoped_local_sql_values_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&field_pair("sql_values_runtime_execution", true)));
        assert!(output.contains(&field_pair("sql_values_generated_source_created", true)));
        assert!(output.contains(&string_field_pair(
            "sql_generate_series_range_blocker_id",
            "none_scoped_local_sql_generate_series_range_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&string_field_pair(
            "sql_source_free_projection_blocker_id",
            "none_scoped_local_sql_range_projection_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&field_pair(
            "sql_source_free_projection_runtime_execution",
            true
        )));
        assert!(output.contains(&field_pair(
            "sql_generate_series_range_runtime_execution",
            true
        )));
        assert!(output.contains(&field_pair(
            "sql_generate_series_range_generated_source_created",
            true
        )));
        assert!(output.contains(&string_field_pair(
            "dataframe_generated_with_column_blocker_id",
            "none_scoped_local_generated_with_column_jsonl_csv_smoke_only"
        )));
        assert!(output.contains(&field_pair(
            "dataframe_generated_with_column_runtime_execution",
            true
        )));
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn wrapper_connector_registry_classifies_api_surface_wrappers_and_connectors() {
    let output = run_capabilities_scope("api-surfaces");

    assert!(output.contains(&string_field_pair(
        "command_registry_schema_version",
        "shardloom.command_registry.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_report_id",
        "review-p1-1.command_registry"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_docs_ref",
        "docs/status/cli-command-registry.md"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_source",
        "shardloom-cli/src/command_registry.rs"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_metadata_command",
        "shardloom command-metadata [command] --format json"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_help_command",
        "shardloom help [command] --format json"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_registered_command_count",
        &command_registry_row_field_ids().len().to_string()
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_support_state_vocabulary",
        "executable,feature_gated,diagnostic_only,report_only,blocked,future"
    )));
    assert!(output.contains(&field_pair("command_registry_fallback_attempted", false)));
    assert!(output.contains(&field_pair(
        "command_registry_external_engine_invoked",
        false
    )));
    assert!(output.contains(&field_pair(
        "command_registry_all_commands_have_usage_fragment",
        true
    )));
    assert!(output.contains(&field_pair(
        "command_registry_all_commands_classified",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_claim_gate_status",
        "metadata_only_not_claim_grade"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_row_help_usage_fragment",
        "help [command]"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_row_vortex_ingest_smoke_input_contract",
        "local_source_or_vortex_artifact_args"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_row_vortex_ingest_smoke_output_contract",
        "typed_envelope_plus_local_runtime_or_artifact_evidence"
    )));
    assert!(output.contains(&string_field_pair(
        "command_registry_row_vortex_ingest_smoke_owning_phase_item",
        "GAR-RUNTIME-IMPL-4"
    )));
    assert!(output.contains(&field_pair(
        "command_registry_row_vortex_ingest_smoke_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "command_registry_row_vortex_ingest_smoke_external_engine_invoked",
        false
    )));

    assert!(output.contains(&string_field_pair(
        "evidence_schema_registry_schema_version",
        "shardloom.evidence_field_schema_registry.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "evidence_schema_registry_command",
        "shardloom evidence-schema [surface] --format json"
    )));
    assert!(output.contains(&string_field_pair(
        "evidence_schema_registry_surface_count",
        "8"
    )));
    assert!(output.contains(&field_pair(
        "evidence_schema_registry_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "evidence_schema_registry_external_engine_invoked",
        false
    )));
    assert!(output.contains(&string_field_pair(
        "evidence_schema_surface_execution_mode_selection_report_python_accessor_mapping",
        "TraditionalAnalyticsRun.execution_mode_selection_fields"
    )));
    assert!(output.contains(&string_field_pair(
        "evidence_schema_field_execution_mode_selection_report_fallback_attempted_no_fallback_semantics",
        "must_remain_false"
    )));

    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_schema_version",
        "shardloom.wrapper_connector_implementation_registry.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_report_id",
        "gar-0037-a.wrapper_connector_implementation_registry"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_row_count",
        "26"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_ready_local_count",
        "3"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_report_only_count",
        "9"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_blocked_count",
        "14"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_dependency_expansion_allowed",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_external_engine_invoked",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_all_rows_no_fallback_no_external_engine",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_row_python_cli_json_client_support_status",
        "ready_local"
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_row_python_cli_json_client_explicit_execution_available",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_row_sqlalchemy_support_status",
        "blocked"
    )));
    assert!(output.contains(&string_field_pair(
        "wrapper_connector_registry_row_flight_sql_deterministic_diagnostic_code",
        "SL_COLUMNAR_TRANSPORT_UNSUPPORTED"
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_row_flight_sql_data_plane_bridge_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_row_mcp_external_engine_invoked",
        false
    )));
    assert!(output.contains(&field_pair(
        "wrapper_connector_registry_row_mcp_fallback_attempted",
        false
    )));
}

#[test]
fn cg20_user_surface_capabilities_expose_evidence_gates() {
    for (scope, dimension, components, native_io_required, adapter_required) in [
        (
            "data-etl",
            "data_etl_surface",
            "ingestion,schema_contracts,data_quality,cleaning,transformation,enrichment,incremental_state,writes_exports,lineage_observability,governance",
            true,
            false,
        ),
        (
            "python",
            "python_surface",
            "thin_cli_json_wrapper,python_api,diagnostics,materialization_boundaries,python_udf_boundaries,package_metadata,wheel_sdist_build,fresh_environment_smoke,conda_wrapper_cli_split",
            false,
            false,
        ),
        (
            "universal-adapters",
            "universal_adapter_catalog",
            "tabular_files,lakehouse_tables,object_stores,catalogs,relational_warehouses,events_apis_saas,python_notebook,unstructured_media",
            false,
            true,
        ),
        (
            "unstructured-media",
            "unstructured_media",
            "document_refs,media_refs,text_extraction,chunk_manifests,provenance,redaction,effect_permissions",
            true,
            false,
        ),
    ] {
        let output = run_capabilities_scope(scope);
        assert!(output.contains(&string_field_pair("dimension", dimension)));
        assert!(output.contains(&string_field_pair(
            "dimension_status",
            "evidence_insufficient"
        )));
        assert!(output.contains(&string_field_pair("surface_components", components)));
        assert!(output.contains(&field_pair(
            "native_io_certificate_required",
            native_io_required
        )));
        assert!(output.contains(&field_pair(
            "adapter_certification_required",
            adapter_required
        )));
        assert!(output.contains(&field_pair("production_claim_allowed", false)));
        assert!(output.contains(&field_pair("best_default_publication_allowed", false)));
    }
}

#[test]
fn certification_capabilities_expose_best_default_gate_without_claims() {
    let output = run_capabilities_scope("certification");

    assert!(output.contains(&string_field_pair(
        "best_default_certification_gate_schema_version",
        "shardloom.best_default_certification_gate.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "best_default_certification_gate_report_id",
        "gar-0032-e.best_default_certification_gate"
    )));
    assert!(output.contains(&string_field_pair(
        "best_default_certification_gate_support_status",
        "blocked"
    )));
    assert!(output.contains(&string_field_pair(
        "best_default_certification_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&string_field_pair(
        "best_default_certification_gate_attached_evidence_refs",
        "none"
    )));
    for key in [
        "best_default_certification_gate_correctness_evidence_required",
        "best_default_certification_gate_benchmark_evidence_required",
        "best_default_certification_gate_execution_certificate_required",
        "best_default_certification_gate_native_io_certificate_required",
        "best_default_certification_gate_materialization_decode_required",
        "best_default_certification_gate_no_fallback_policy_required",
        "best_default_certification_gate_release_security_required",
        "best_default_certification_gate_ux_install_docs_required",
    ] {
        assert!(
            output.contains(&field_pair(key, true)),
            "missing required gate field {key}"
        );
    }
    for key in [
        "best_default_certification_gate_all_required_evidence_attached",
        "best_default_language_allowed",
        "best_default_certification_gate_best_default_claim_allowed",
        "best_default_certification_gate_performance_claim_allowed",
        "best_default_certification_gate_superiority_claim_allowed",
        "best_default_certification_gate_spark_replacement_claim_allowed",
        "best_default_certification_gate_production_claim_allowed",
        "best_default_certification_gate_runtime_execution",
        "best_default_certification_gate_fallback_attempted",
        "best_default_certification_gate_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field_pair(key, false)),
            "missing false gate field {key}"
        );
    }
}

#[test]
fn operator_capability_discovery_includes_physical_plan_blockers() {
    let output = run_capabilities_scope("operators");

    assert_operator_discovery_physical_plan(&output);
    assert_operator_discovery_metadata_kernel(&output);
    assert_operator_discovery_metadata_count_kernel_admission(&output);
    assert_operator_discovery_metadata_filter_kernel_admission(&output);
    assert_operator_discovery_metadata_projection_kernel_admission(&output);
    assert_operator_discovery_encoded_projection_kernel_admission(&output);
    assert_operator_discovery_encoded_count_kernel(&output);
    assert_operator_discovery_encoded_predicate_evaluation(&output);
    assert_operator_discovery_selection_vector_filter_kernel(&output);
    assert_operator_discovery_encoded_count_guard(&output);
    assert_operator_discovery_local_vortex_primitive_execution(&output);
}

#[test]
fn engine_capability_discovery_exposes_cg22_contract_without_runtime_claims() {
    let output = run_capabilities_scope("engines");

    assert!(output.contains(&string_field_pair(
        "engine_capability_schema_version",
        "shardloom.engine_capability_matrix.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "engine_mode_vocabulary",
        "batch,live,hybrid,auto"
    )));
    assert!(output.contains(&string_field_pair(
        "boundedness_vocabulary",
        "bounded,unbounded,snapshot,unknown"
    )));
    assert!(output.contains(&string_field_pair(
        "batch_support_status",
        "partially_supported"
    )));
    assert!(output.contains(&string_field_pair(
        "live_support_status",
        "partially_supported"
    )));
    assert!(output.contains(&string_field_pair(
        "hybrid_support_status",
        "partially_supported"
    )));
    assert!(output.contains(&string_field_pair("partially_supported_engine_count", "3")));
    assert!(output.contains(&string_field_pair("planned_engine_count", "0")));
    assert!(output.contains(&field_pair("batch_production_claim_allowed", false)));
    assert!(output.contains(&field_pair("live_production_claim_allowed", false)));
    assert!(output.contains(&field_pair("hybrid_production_claim_allowed", false)));
    assert!(output.contains(&string_field_pair(
        "streaming_capability_matrix_report_id",
        "gar0013.streaming_runtime_capability_matrix"
    )));
    assert!(output.contains(&string_field_pair(
        "streaming_capability_matrix_row_count",
        "8"
    )));
    assert!(output.contains(&string_field_pair(
        "streaming_capability_matrix_diagnostic_code_order",
        "SL_OBJECT_STORE_UNSUPPORTED,SL_MATERIALIZATION_REQUIRED,SL_NOT_IMPLEMENTED"
    )));
    assert!(output.contains(&field_pair(
        "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
        true
    )));
    assert!(output.contains(&string_field_pair(
        "live_hybrid_fabric_gate_schema_version",
        "shardloom.live_hybrid_fabric_freshness_gate.v1"
    )));
    assert!(output.contains(&string_field_pair(
        "live_hybrid_fabric_gate_report_id",
        "gar-0034-a.live_hybrid_fabric_freshness_gate"
    )));
    assert!(output.contains(&string_field_pair(
        "live_hybrid_fabric_gate_row_order",
        "live_broker_adapter,live_durable_checkpoint_store,live_unbounded_scheduler,live_freshness_certificate,live_exactly_once_claim,hybrid_micro_segment_flush,hybrid_object_store_commit,hybrid_catalog_snapshot,baseline_oracle_boundary"
    )));
    assert!(output.contains(&string_field_pair(
        "live_hybrid_fabric_gate_blocked_row_count",
        "7"
    )));
    assert!(output.contains(&string_field_pair(
        "live_hybrid_fabric_gate_fixture_smoke_row_count",
        "1"
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_freshness_claim_allowed",
        false
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_exactly_once_claim_allowed",
        false
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_object_store_runtime_supported",
        false
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_baseline_oracle_only",
        true
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_fallback_attempted",
        false
    )));
    assert!(output.contains(&field_pair(
        "live_hybrid_fabric_gate_external_engine_invoked",
        false
    )));
    assert!(output.contains(&field_pair("live_state_required", true)));
    assert!(output.contains(&field_pair("hybrid_checkpoint_required", true)));
    assert!(output.contains(&string_field_pair("severity", "error")));
    assert!(output.contains(&string_field_pair(
        "batch_blocker_ids",
        "cg22.engine.batch.workload_correctness_evidence,cg22.engine.batch.benchmark_evidence,cg22.engine.batch.broad_source_sink_certification"
    )));
    assert!(output.contains(&field_pair("batch_no_runtime", true)));
    assert!(output.contains(&field_pair("live_no_fallback", true)));
    assert!(output.contains(&field_pair("hybrid_no_effects", true)));
}

#[test]
fn cross_cg_capability_parity_surfaces_shared_blocker_contracts() {
    let workflow = run_capabilities_scope("workflow");
    let remote_api = run_capabilities_scope("remote-api");
    let cross_cg = run_capabilities_scope("cross-cg");

    assert!(workflow.contains(&string_field_pair(
        "schema_version",
        "shardloom.workflow_capability_parity.v1"
    )));
    assert!(workflow.contains(&string_field_pair("workflow_operation_count", "45")));
    assert!(workflow.contains(&string_field_pair(
        "workflow_operation_names",
        "profile,collect,from_pandas,from_arrow_table,from_arrow_ipc,to_pandas,to_arrow,to_arrow_table,to_arrow_ipc,to_numpy,to_python_objects,with_column,group_by,agg,sort,limit,write_vortex,write_parquet,write_arrow_ipc,write_avro,write_orc,sql,sql_parse,sql_bind,sql_plan,sql_execute,sql_source_free_projection,dataframe_source_free_projection,dataframe_generated_with_column,object_store_generated_output,foundry_generated_output,join,aggregate,window,schema_contract,schema,describe_schema,validate_schema,data_quality,data_quality_summary,quarantine,preview,display,object_store_read,fallback_engine"
    )));
    assert!(workflow.contains(&string_field_pair(
        "blocker_ids",
        "cg21.workflow.profile.runtime_profile_unsupported,cg21.workflow.collect.materialization_unsupported,cg21.workflow.from_pandas.materialized_input_unsupported,cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported,cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported,cg21.workflow.to_pandas.decoded_dataframe_unsupported,cg21.workflow.to_arrow.decoded_columnar_unsupported,cg21.workflow.to_arrow_table.decoded_table_unsupported,cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported,cg21.workflow.to_numpy.python_array_unsupported,cg21.workflow.to_python_objects.object_materialization_unsupported,cg21.workflow.with_column.expression_unsupported,cg21.workflow.group_by.operator_unsupported,cg21.workflow.agg.operator_unsupported,cg21.workflow.sort.operator_unsupported,cg21.workflow.limit.execution_uncertified,cg21.workflow.write_vortex.write_policy_unsupported,cg21.workflow.write_parquet.compatibility_export_unsupported,cg21.workflow.write_arrow_ipc.compatibility_export_unsupported,cg21.workflow.write_avro.compatibility_export_unsupported,cg21.workflow.write_orc.compatibility_export_unsupported,cg21.workflow.sql.frontend_unsupported,cg21.workflow.sql.parse_unsupported,cg21.workflow.sql.bind_unsupported,cg21.workflow.sql.plan_unsupported,cg21.workflow.sql.execute_unsupported,gar-gen-1.sql_source_free_projection_broad_runtime_blocked,gar-gen-1.dataframe_source_free_projection_runtime_not_implemented,gar-gen-1.dataframe_generated_with_column_broad_expression_runtime_blocked,gar-gen-1.object_store_generated_output_blocked,gar-gen-1.foundry_generated_output_runtime_not_implemented,cg21.workflow.join.operator_unsupported,cg21.workflow.aggregate.operator_unsupported,cg21.workflow.window.operator_unsupported,cg21.workflow.schema_contract.enforcement_unsupported,cg21.workflow.schema.discovery_unsupported,cg21.workflow.describe_schema.report_unsupported,cg21.workflow.validate_schema.validation_unsupported,cg21.workflow.data_quality.checks_unsupported,cg21.workflow.data_quality_summary.report_unsupported,cg21.workflow.quarantine.output_unsupported,cg21.workflow.preview.materialization_unsupported,cg21.workflow.display.rich_display_unsupported,cg21.workflow.object_store_read.runtime_unsupported,cg21.workflow.fallback_engine.no_fallback_policy"
    )));
    assert!(workflow.contains(&string_field_pair("severity", "error")));
    assert!(workflow.contains(&field_pair("no_runtime", true)));
    assert!(workflow.contains(&field_pair("no_fallback", true)));
    assert!(workflow.contains(&field_pair("no_effects", true)));
    assert!(workflow.contains(&string_field_pair(
        "etl_workflow_matrix_schema_version",
        "shardloom.etl_workflow_capability_matrix.v1"
    )));
    assert!(workflow.contains(&string_field_pair(
        "etl_workflow_matrix_id",
        "gar-0033-a.etl_workflow_capability_matrix"
    )));
    assert!(workflow.contains(&string_field_pair(
        "etl_workflow_row_order",
        "first_10_minutes_local_smoke,local_csv_parquet_certified_workload,prepared_native_vortex_batch_smoke,source_free_user_rows_jsonl_csv,source_free_range_jsonl_csv,source_free_literal_table_jsonl_csv,source_free_calendar_jsonl_csv,dirty_csv_fixture,nested_json_fixture,cdc_overlay_fixture,sql_dataframe_capability_posture,data_quality_api,object_store_runtime,table_lakehouse_runtime,production_etl_certification"
    )));
    assert!(workflow.contains(&string_field_pair(
        "etl_workflow_supported_local_count",
        "10"
    )));
    assert!(workflow.contains(&string_field_pair("etl_workflow_report_only_count", "2")));
    assert!(workflow.contains(&string_field_pair("etl_workflow_blocked_count", "3")));
    assert!(workflow.contains(&string_field_pair(
        "etl_workflow_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(workflow.contains(&field_pair("etl_workflow_fallback_attempted", false)));
    assert!(workflow.contains(&field_pair("etl_workflow_external_engine_invoked", false)));
    assert!(workflow.contains(&field_pair(
        "etl_workflow_production_etl_claim_allowed",
        false
    )));
    assert!(workflow.contains(&field_pair(
        "etl_workflow_object_store_runtime_supported",
        false
    )));
    assert!(workflow.contains(&field_pair(
        "etl_workflow_table_lakehouse_runtime_supported",
        false
    )));

    assert!(remote_api.contains(&string_field_pair(
        "schema_version",
        "shardloom.remote_api_capability_parity.v1"
    )));
    assert!(remote_api.contains(&string_field_pair(
        "blocker_ids",
        "cg23.remote_api.plan_preview.unsupported_operator,cg23.remote_api.remote_object_store.unsupported,cg23.remote_api.lifecycle.uncertified_blocked,cg23.remote_api.data_plane.materialization_boundary_required"
    )));
    assert!(remote_api.contains(&string_field_pair(
        "suggested_next_action",
        "Use rest-api-contract-plan and rest-api-plan-preview for scenario-specific blockers before enabling remote execution."
    )));

    assert!(cross_cg.contains(&string_field_pair("represented_gates", "cg21,cg22,cg23")));
    assert!(cross_cg.contains(&string_field_pair("severity", "error")));
    assert!(cross_cg.contains(&string_field_pair(
        "blocker_ids",
        "cg21.workflow.profile.runtime_profile_unsupported,cg21.workflow.collect.materialization_unsupported,cg21.workflow.from_pandas.materialized_input_unsupported,cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported,cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported,cg21.workflow.to_pandas.decoded_dataframe_unsupported,cg21.workflow.to_arrow.decoded_columnar_unsupported,cg21.workflow.to_arrow_table.decoded_table_unsupported,cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported,cg21.workflow.to_numpy.python_array_unsupported,cg21.workflow.to_python_objects.object_materialization_unsupported,cg21.workflow.with_column.expression_unsupported,cg21.workflow.group_by.operator_unsupported,cg21.workflow.agg.operator_unsupported,cg21.workflow.sort.operator_unsupported,cg21.workflow.limit.execution_uncertified,cg21.workflow.write_vortex.write_policy_unsupported,cg21.workflow.write_parquet.compatibility_export_unsupported,cg21.workflow.write_arrow_ipc.compatibility_export_unsupported,cg21.workflow.write_avro.compatibility_export_unsupported,cg21.workflow.write_orc.compatibility_export_unsupported,cg21.workflow.sql.frontend_unsupported,cg21.workflow.sql.parse_unsupported,cg21.workflow.sql.bind_unsupported,cg21.workflow.sql.plan_unsupported,cg21.workflow.sql.execute_unsupported,gar-gen-1.sql_source_free_projection_broad_runtime_blocked,gar-gen-1.dataframe_source_free_projection_runtime_not_implemented,gar-gen-1.dataframe_generated_with_column_broad_expression_runtime_blocked,gar-gen-1.object_store_generated_output_blocked,gar-gen-1.foundry_generated_output_runtime_not_implemented,cg21.workflow.join.operator_unsupported,cg21.workflow.aggregate.operator_unsupported,cg21.workflow.window.operator_unsupported,cg21.workflow.schema_contract.enforcement_unsupported,cg21.workflow.schema.discovery_unsupported,cg21.workflow.describe_schema.report_unsupported,cg21.workflow.validate_schema.validation_unsupported,cg21.workflow.data_quality.checks_unsupported,cg21.workflow.data_quality_summary.report_unsupported,cg21.workflow.quarantine.output_unsupported,cg21.workflow.preview.materialization_unsupported,cg21.workflow.display.rich_display_unsupported,cg21.workflow.object_store_read.runtime_unsupported,cg21.workflow.fallback_engine.no_fallback_policy,cg22.engine.batch.workload_correctness_evidence,cg22.engine.batch.benchmark_evidence,cg22.engine.batch.broad_source_sink_certification,cg22.engine.live.external_broker_adapters,cg22.engine.live.durable_checkpoint_store,cg22.engine.live.unbounded_runtime_scheduler,cg22.engine.live.workload_correctness_evidence,cg22.engine.live.benchmark_evidence,cg22.engine.hybrid.durable_micro_segment_flush_writes,cg22.engine.hybrid.object_store_commit_protocol,cg22.engine.hybrid.external_catalog_snapshot_discovery,cg22.engine.hybrid.workload_correctness_evidence,cg22.engine.hybrid.benchmark_evidence,cg23.remote_api.plan_preview.unsupported_operator,cg23.remote_api.remote_object_store.unsupported,cg23.remote_api.lifecycle.uncertified_blocked,cg23.remote_api.data_plane.materialization_boundary_required"
    )));
    assert!(cross_cg.contains(&string_field_pair(
        "required_evidence",
        "execution_certificate,native_io_certificate,operator_capability_matrix,semantic_conformance_suite,sql_parser,binder,write_intent,rest_api_contract,decoded_columnar_boundary,python_object_boundary,schema_metadata_report,data_quality_report,notebook_display_boundary,object_store_capability_policy,credential_policy,no_fallback_policy,workload_correctness_evidence,benchmark_evidence,broad_source_sink_certification,durable_checkpoint_store,object_store_commit_protocol,openapi_contract,asyncapi_contract,execution_certificate,native_io_certificate,security_governance_policy,data_plane_fidelity_report"
    )));
    assert!(cross_cg.contains(&string_field_pair(
        "suggested_next_action",
        "Use workflow-unsupported-plan for method-specific blocker details before requesting execution. Use engine-selection-plan and engine-capability-matrix before making engine-mode execution claims. Use rest-api-contract-plan and rest-api-plan-preview for scenario-specific blockers before enabling remote execution."
    )));
    assert!(cross_cg.contains(&string_field_pair(
        "cg21_workflow_diagnostic_surface",
        "workflow-unsupported-plan"
    )));
    assert!(cross_cg.contains(&string_field_pair(
        "cg22_engine_modes_diagnostic_surface",
        "engine-capability-matrix"
    )));
    assert!(cross_cg.contains(&string_field_pair(
        "cg23_remote_api_diagnostic_surface",
        "rest-api-plan-preview"
    )));
    assert!(cross_cg.contains(&field_pair("cg21_workflow_no_runtime", true)));
    assert!(cross_cg.contains(&field_pair("cg22_engine_modes_no_fallback", true)));
    assert!(cross_cg.contains(&field_pair("cg23_remote_api_no_effects", true)));
}

fn assert_operator_discovery_physical_plan(output: &str) {
    assert!(output.contains(
        "{\"key\":\"physical_operator_schema_version\",\"value\":\"shardloom.physical_operator_plan.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"physical_operator_plan_id\",\"value\":\"runtime.5g-f1-physical-operator-kernel-coverage\"}"
    ));
    assert!(
        output.contains("{\"key\":\"operator_coverage_report_id\",\"value\":\"runtime.5g-f1\"}")
    );
    assert!(output.contains("{\"key\":\"operator_encoded_capable_count\",\"value\":\"3\"}"));
    assert!(output.contains("{\"key\":\"operator_native_decoded_count\",\"value\":\"10\"}"));
    assert!(output.contains("{\"key\":\"operator_planned_native_count\",\"value\":\"3\"}"));
    assert!(output.contains("{\"key\":\"operator_unsupported_count\",\"value\":\"11\"}"));
    assert!(output.contains("{\"key\":\"physical_operator_count\",\"value\":\"12\"}"));
    assert!(output.contains("{\"key\":\"physical_operator_ready_count\",\"value\":\"10\"}"));
    assert!(
        output.contains("{\"key\":\"physical_operator_missing_kernel_count\",\"value\":\"0\"}")
    );
    assert!(output.contains("{\"key\":\"physical_operator_unsupported_count\",\"value\":\"2\"}"));
    assert!(output.contains(
        "{\"key\":\"physical_operator_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(
        output.contains("{\"key\":\"physical_operator_runtime_execution\",\"value\":\"false\"}")
    );
    assert!(output.contains(
        "{\"key\":\"physical_operator_execution_profile_schema_version\",\"value\":\"shardloom.physical_operator_execution_profiles.v1\"}"
    ));
    assert!(
        output.contains("{\"key\":\"physical_operator_execution_profile_count\",\"value\":\"12\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"physical_operator_native_execution_level_count\",\"value\":\"4\"}"
        )
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_metadata_only_level_count\",\"value\":\"4\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_encoded_native_level_count\",\"value\":\"3\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_hybrid_native_level_count\",\"value\":\"5\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_native_decoded_level_count\",\"value\":\"8\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_reference_only_level_count\",\"value\":\"0\"}")
    );
    assert!(
        output.contains("{\"key\":\"physical_operator_fallback_level_count\",\"value\":\"0\"}")
    );
}

fn assert_operator_discovery_metadata_kernel(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_schema_version\",\"value\":\"shardloom.vortex_metadata_physical_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_supported_primitives\",\"value\":\"count_all,count_where,filter_predicate\"}"
    ));
    assert!(
        output
            .contains("{\"key\":\"metadata_physical_kernel_contextual_only\",\"value\":\"true\"}")
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(
        output.contains("{\"key\":\"metadata_physical_kernel_data_read\",\"value\":\"false\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"metadata_physical_kernel_runtime_execution\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_count_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_count_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_metadata_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_filter_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_filter_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_operator_kind\",\"value\":\"filter\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_metadata_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_projection_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_projection_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_operator_kind\",\"value\":\"project\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_projection_readiness\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_projection_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_encoded_projection_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_operator_kind\",\"value\":\"project\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_required_kernel_kind\",\"value\":\"encoded\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_requires_projection_readiness\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_requires_encoded_column_path\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_projection_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_count_kernel(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_schema_version\",\"value\":\"shardloom.vortex_encoded_count_physical_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_id\",\"value\":\"vortex.query-primitive.count_all.encoded-count-physical-kernel\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_supported_primitive\",\"value\":\"count_all\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_physical_kernel_kernel_kind\",\"value\":\"encoded\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_execution_level\",\"value\":\"encoded_native\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_execution_certificate\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_discovery_reads_data\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_evaluated_path_reads_data\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_encoded_count_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_required_kernel_kind\",\"value\":\"encoded\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_physical_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_predicate_evaluation(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_schema_version\",\"value\":\"shardloom.vortex_encoded_predicate_evaluation.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_id\",\"value\":\"vortex.query-primitive.filter_predicate.encoded-predicate-evaluation\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_predicate_evaluation_operator_kind\",\"value\":\"filter\"}"
        )
    );
    assert!(
        output.contains(
            "{\"key\":\"encoded_predicate_evaluation_kernel_kind\",\"value\":\"encoded\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_execution_level\",\"value\":\"encoded_native\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_predicate_evaluation_contextual_only\",\"value\":\"true\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_emits_selection_vectors\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_defers_inconclusive_to_encoded_values\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_discovery_reads_data\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_predicate_evaluation_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_selection_vector_filter_kernel(output: &str) {
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_schema_version\",\"value\":\"shardloom.vortex_selection_vector_filter_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_id\",\"value\":\"vortex.query-primitive.filter_predicate.selection-vector-filter-kernel\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_operator_kind\",\"value\":\"filter\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_kernel_kind\",\"value\":\"encoded\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_execution_level\",\"value\":\"encoded_native\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_requires_encoded_predicate_evaluation\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_requires_selection_vectors\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_discovery_reads_data\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_selection_vector_filter_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_admission_required_kernel_kind\",\"value\":\"encoded\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_admission_requires_filter_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"selection_vector_filter_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_count_guard(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_schema_version\",\"value\":\"shardloom.vortex_encoded_count_local_guard.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_id\",\"value\":\"cg2.1e-layout-approved-count-local-guard\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_accepted_approval_sources\",\"value\":\"execution_usable_public_api_boundary,layout_row_count_approval,approved_local_scan_execution_report\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_local_execution_status\",\"value\":\"needs_encoded_read\"}"
    ));
    assert!(
        output.contains("{\"key\":\"encoded_count_local_guard_mode\",\"value\":\"plan_only\"}")
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_layout_row_count_path_accepted\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_approved_local_scan_result_bridge_available\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_returns_count_result\",\"value\":\"false\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_local_guard_side_effect_free\",\"value\":\"true\"}"
        )
    );
    assert!(
        output.contains("{\"key\":\"encoded_count_local_guard_data_read\",\"value\":\"false\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_local_guard_runtime_execution\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_local_vortex_primitive_execution(output: &str) {
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_schema_version\",\"value\":\"shardloom.vortex_local_primitive_execution.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_feature_gate\",\"value\":\"vortex-local-primitives\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_supported_primitives\",\"value\":\"count_all,count_where,filter_predicate,project_columns,filter_and_project\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"local_vortex_primitive_execution_local_only\",\"value\":\"true\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_count_all_decode_required\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_filter_project_decode_boundary_reported\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_scan_filter_pushdown\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_scan_projection_pushdown\",\"value\":\"true\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"local_vortex_primitive_execution_row_read\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_arrow_converted\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_object_store_io\",\"value\":\"false\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"local_vortex_primitive_execution_write_io\",\"value\":\"false\"}"
        )
    );
    assert!(
        output.contains(
            "{\"key\":\"local_vortex_primitive_execution_spill_io\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"local_vortex_primitive_execution_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn run_capabilities_scope(scope: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["capabilities", scope, "--format", "json"])
        .output()
        .expect("shardloom binary executes");

    assert!(
        output.status.success(),
        "scope={scope} stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "scope={scope} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_runs_today() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["runs-today", "--format", "json"])
        .output()
        .expect("shardloom binary executes");

    assert!(
        output.status.success(),
        "runs-today stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "runs-today stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field_keys(output: &str) -> Vec<&str> {
    let (_, top_level_fields) = output
        .rsplit_once("\"fields\":[")
        .expect("top-level legacy fields mirror is present");
    top_level_fields
        .split("{\"key\":\"")
        .skip(1)
        .map(|part| {
            part.split_once('"').map_or_else(
                || panic!("field key terminator missing in {part}"),
                |(key, _)| key,
            )
        })
        .collect()
}

fn field_pair(key: &str, value: bool) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn string_field_pair(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}
