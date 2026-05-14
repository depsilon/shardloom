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

const SQL_FIELD_KEYS: [&str; 35] = [
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
];

const FUNCTION_FIELD_KEYS: [&str; 13] = [
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
    "function_group_count",
    "planned_count",
];

const ENGINE_FIELD_KEYS: [&str; 65] = [
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

const WORKFLOW_FIELD_KEYS: [&str; 30] = [
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

const OPERATOR_FIELD_KEYS: [&str; 180] = [
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
    "operator_family_count",
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

const CERTIFICATION_FIELD_KEYS: [&str; 16] = [
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
];

const WORLD_CLASS_SURFACE_FIELD_KEYS: [&str; 24] = [
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

const DATAFRAME_WORLD_CLASS_SURFACE_FIELD_KEYS: [&str; 45] = [
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
        ("sql", SQL_FIELD_KEYS.as_slice()),
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
    for scope in WORLD_CLASS_SURFACE_SCOPES {
        let output = run_capabilities_scope(scope);
        let keys = field_keys(&output);
        let expected_keys = if scope == "dataframe" {
            DATAFRAME_WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice()
        } else {
            WORLD_CLASS_SURFACE_FIELD_KEYS.as_slice()
        };
        assert_eq!(keys.as_slice(), expected_keys, "scope={scope}");
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
fn sql_and_dataframe_capabilities_expose_planner_readiness_matrix() {
    for scope in ["sql", "dataframe"] {
        let output = run_capabilities_scope(scope);
        for key in PLANNER_READINESS_FIELD_KEYS {
            assert!(
                output.contains(&format!("{{\"key\":\"{key}\",\"value\":")),
                "scope={scope} missing key={key}"
            );
        }
        assert!(output.contains(&string_field_pair(
            "planner_readiness_schema_version",
            "shardloom.sql_dataframe_planner_readiness.v1"
        )));
        assert!(output.contains(&string_field_pair(
            "planner_readiness_claim_gate_status",
            "not_claim_grade"
        )));
        assert!(output.contains(&string_field_pair(
            "planner_readiness_sql_row_order",
            "sql_text_admission,sql_parse,sql_bind,sql_plan,sql_execute"
        )));
        assert!(output.contains(&string_field_pair(
            "planner_readiness_dataframe_row_order",
            "dataframe_lazy_plan,dataframe_expression_builder,dataframe_join,dataframe_aggregate,dataframe_window"
        )));
        assert!(output.contains(&string_field_pair(
            "planner_readiness_unsupported_diagnostic_codes",
            "SL_SQL_TEXT_ADMISSION_REPORT_ONLY,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_DATAFRAME_LAZY_PLAN_REPORT_ONLY,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_UNSUPPORTED_SQL,SL_PLANNER_READINESS_DIAGNOSTICS_REPORT_ONLY,SL_UNSUPPORTED_PLANNER_EXECUTION_STATE"
        )));
        assert!(output.contains(&field_pair("planner_readiness_parser_executed", false)));
        assert!(output.contains(&field_pair("planner_readiness_binder_executed", false)));
        assert!(output.contains(&field_pair("planner_readiness_planner_executed", false)));
        assert!(output.contains(&field_pair("planner_readiness_runtime_execution", false)));
        assert!(output.contains(&field_pair("planner_readiness_dataframe_runtime", false)));
        assert!(output.contains(&field_pair(
            "planner_readiness_external_engine_invoked",
            false
        )));
        assert!(output.contains(&field_pair("planner_readiness_fallback_attempted", false)));
        assert!(output.contains(&field_pair(
            "planner_readiness_deterministic_diagnostics_present",
            true
        )));
    }
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
    assert!(workflow.contains(&string_field_pair("workflow_operation_count", "37")));
    assert!(workflow.contains(&string_field_pair(
        "workflow_operation_names",
        "profile,collect,from_pandas,from_arrow_table,from_arrow_ipc,to_pandas,to_arrow,to_arrow_table,to_arrow_ipc,to_numpy,to_python_objects,with_column,group_by,agg,sort,limit,write_vortex,write_parquet,sql,sql_parse,sql_bind,sql_plan,sql_execute,join,aggregate,window,schema_contract,schema,describe_schema,validate_schema,data_quality,data_quality_summary,quarantine,preview,display,object_store_read,fallback_engine"
    )));
    assert!(workflow.contains(&string_field_pair(
        "blocker_ids",
        "cg21.workflow.profile.runtime_profile_unsupported,cg21.workflow.collect.materialization_unsupported,cg21.workflow.from_pandas.materialized_input_unsupported,cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported,cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported,cg21.workflow.to_pandas.decoded_dataframe_unsupported,cg21.workflow.to_arrow.decoded_columnar_unsupported,cg21.workflow.to_arrow_table.decoded_table_unsupported,cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported,cg21.workflow.to_numpy.python_array_unsupported,cg21.workflow.to_python_objects.object_materialization_unsupported,cg21.workflow.with_column.expression_unsupported,cg21.workflow.group_by.operator_unsupported,cg21.workflow.agg.operator_unsupported,cg21.workflow.sort.operator_unsupported,cg21.workflow.limit.execution_uncertified,cg21.workflow.write_vortex.write_policy_unsupported,cg21.workflow.write_parquet.compatibility_export_unsupported,cg21.workflow.sql.frontend_unsupported,cg21.workflow.sql.parse_unsupported,cg21.workflow.sql.bind_unsupported,cg21.workflow.sql.plan_unsupported,cg21.workflow.sql.execute_unsupported,cg21.workflow.join.operator_unsupported,cg21.workflow.aggregate.operator_unsupported,cg21.workflow.window.operator_unsupported,cg21.workflow.schema_contract.enforcement_unsupported,cg21.workflow.schema.discovery_unsupported,cg21.workflow.describe_schema.report_unsupported,cg21.workflow.validate_schema.validation_unsupported,cg21.workflow.data_quality.checks_unsupported,cg21.workflow.data_quality_summary.report_unsupported,cg21.workflow.quarantine.output_unsupported,cg21.workflow.preview.materialization_unsupported,cg21.workflow.display.rich_display_unsupported,cg21.workflow.object_store_read.runtime_unsupported,cg21.workflow.fallback_engine.no_fallback_policy"
    )));
    assert!(workflow.contains(&string_field_pair("severity", "error")));
    assert!(workflow.contains(&field_pair("no_runtime", true)));
    assert!(workflow.contains(&field_pair("no_fallback", true)));
    assert!(workflow.contains(&field_pair("no_effects", true)));

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
        "cg21.workflow.profile.runtime_profile_unsupported,cg21.workflow.collect.materialization_unsupported,cg21.workflow.from_pandas.materialized_input_unsupported,cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported,cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported,cg21.workflow.to_pandas.decoded_dataframe_unsupported,cg21.workflow.to_arrow.decoded_columnar_unsupported,cg21.workflow.to_arrow_table.decoded_table_unsupported,cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported,cg21.workflow.to_numpy.python_array_unsupported,cg21.workflow.to_python_objects.object_materialization_unsupported,cg21.workflow.with_column.expression_unsupported,cg21.workflow.group_by.operator_unsupported,cg21.workflow.agg.operator_unsupported,cg21.workflow.sort.operator_unsupported,cg21.workflow.limit.execution_uncertified,cg21.workflow.write_vortex.write_policy_unsupported,cg21.workflow.write_parquet.compatibility_export_unsupported,cg21.workflow.sql.frontend_unsupported,cg21.workflow.sql.parse_unsupported,cg21.workflow.sql.bind_unsupported,cg21.workflow.sql.plan_unsupported,cg21.workflow.sql.execute_unsupported,cg21.workflow.join.operator_unsupported,cg21.workflow.aggregate.operator_unsupported,cg21.workflow.window.operator_unsupported,cg21.workflow.schema_contract.enforcement_unsupported,cg21.workflow.schema.discovery_unsupported,cg21.workflow.describe_schema.report_unsupported,cg21.workflow.validate_schema.validation_unsupported,cg21.workflow.data_quality.checks_unsupported,cg21.workflow.data_quality_summary.report_unsupported,cg21.workflow.quarantine.output_unsupported,cg21.workflow.preview.materialization_unsupported,cg21.workflow.display.rich_display_unsupported,cg21.workflow.object_store_read.runtime_unsupported,cg21.workflow.fallback_engine.no_fallback_policy,cg22.engine.batch.workload_correctness_evidence,cg22.engine.batch.benchmark_evidence,cg22.engine.batch.broad_source_sink_certification,cg22.engine.live.external_broker_adapters,cg22.engine.live.durable_checkpoint_store,cg22.engine.live.unbounded_runtime_scheduler,cg22.engine.live.workload_correctness_evidence,cg22.engine.live.benchmark_evidence,cg22.engine.hybrid.durable_micro_segment_flush_writes,cg22.engine.hybrid.object_store_commit_protocol,cg22.engine.hybrid.external_catalog_snapshot_discovery,cg22.engine.hybrid.workload_correctness_evidence,cg22.engine.hybrid.benchmark_evidence,cg23.remote_api.plan_preview.unsupported_operator,cg23.remote_api.remote_object_store.unsupported,cg23.remote_api.lifecycle.uncertified_blocked,cg23.remote_api.data_plane.materialization_boundary_required"
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
        "{\"key\":\"physical_operator_plan_id\",\"value\":\"cg7.1-physical-operator-foundation\"}"
    ));
    assert!(output.contains("{\"key\":\"physical_operator_count\",\"value\":\"3\"}"));
    assert!(output.contains("{\"key\":\"physical_operator_ready_count\",\"value\":\"0\"}"));
    assert!(
        output.contains("{\"key\":\"physical_operator_missing_kernel_count\",\"value\":\"3\"}")
    );
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
        output.contains("{\"key\":\"physical_operator_execution_profile_count\",\"value\":\"3\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"physical_operator_native_execution_level_count\",\"value\":\"4\"}"
        )
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_metadata_only_level_count\",\"value\":\"3\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_encoded_native_level_count\",\"value\":\"3\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_hybrid_native_level_count\",\"value\":\"3\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_native_decoded_level_count\",\"value\":\"3\"}")
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
