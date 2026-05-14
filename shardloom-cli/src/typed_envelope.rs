//! Shared typed-envelope routing for CLI command output.
//!
//! This module is deliberately protocol-only: it routes existing command fields
//! into typed envelope slots and refs without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use shardloom_core::{OutputEnvelope, OutputTypedArtifact, OutputTypedRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedEnvelopeFieldSlot {
    Result,
    Policy,
    Lifecycle,
    CapabilitySnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedEnvelopeRefSlot {
    ResultRef,
    ArtifactRef,
    Certificate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineReportPayloadSpec {
    artifact_id_fallback: &'static str,
    artifact_kind: &'static str,
    status_key: &'static str,
    payload_keys: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlinePrefixedPayloadSpec {
    key_prefix: &'static str,
    artifact_kind: &'static str,
    emitted_key: &'static str,
    id_key: &'static str,
    status_key: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineFieldSubsetPayloadSpec {
    artifact_kind: &'static str,
    emitted_key: &'static str,
    id_key: &'static str,
    status_key: &'static str,
    payload_keys: &'static [&'static str],
}

const INLINE_PREFIXED_PAYLOAD_SPECS: &[InlinePrefixedPayloadSpec] = &[
    InlinePrefixedPayloadSpec {
        key_prefix: "local_count_native_io_",
        artifact_kind: "native_io_certificate",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_certificate_id",
        status_key: "local_count_native_io_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "execution_certificate_",
        artifact_kind: "execution_certificate",
        emitted_key: "execution_certificate_emitted",
        id_key: "execution_certificate_id",
        status_key: "execution_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "local_primitive_native_io_",
        artifact_kind: "native_io_certificate",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_certificate_id",
        status_key: "local_primitive_native_io_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "local_primitive_execution_certificate_",
        artifact_kind: "execution_certificate",
        emitted_key: "local_primitive_execution_certificate_emitted",
        id_key: "local_primitive_execution_certificate_id",
        status_key: "local_primitive_execution_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "streaming_batch_runtime_",
        artifact_kind: "streaming_batch_runtime_report",
        emitted_key: "streaming_batch_runtime_report_emitted",
        id_key: "streaming_batch_runtime_report_id",
        status_key: "streaming_batch_runtime_status",
    },
];

const INLINE_FIELD_SUBSET_PAYLOAD_SPECS: &[InlineFieldSubsetPayloadSpec] = &[
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "source_report",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_source_report_id",
        status_key: "local_count_native_io_certificate_status",
        payload_keys: LOCAL_COUNT_NATIVE_IO_SOURCE_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "source_pushdown_report",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_source_pushdown_report_id",
        status_key: "local_count_native_io_certificate_status",
        payload_keys: LOCAL_COUNT_NATIVE_IO_SOURCE_PUSHDOWN_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "sink_report",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_sink_report_id",
        status_key: "local_count_native_io_certificate_status",
        payload_keys: LOCAL_COUNT_NATIVE_IO_SINK_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "adapter_fidelity_report",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_adapter_fidelity_report_id",
        status_key: "local_count_native_io_certificate_status",
        payload_keys: LOCAL_COUNT_NATIVE_IO_ADAPTER_FIDELITY_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "source_report",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_source_report_id",
        status_key: "local_primitive_native_io_certificate_status",
        payload_keys: LOCAL_PRIMITIVE_NATIVE_IO_SOURCE_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "source_pushdown_report",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_source_pushdown_report_id",
        status_key: "local_primitive_native_io_certificate_status",
        payload_keys: LOCAL_PRIMITIVE_NATIVE_IO_SOURCE_PUSHDOWN_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "sink_report",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_sink_report_id",
        status_key: "local_primitive_native_io_certificate_status",
        payload_keys: LOCAL_PRIMITIVE_NATIVE_IO_SINK_REPORT_PAYLOAD_KEYS,
    },
    InlineFieldSubsetPayloadSpec {
        artifact_kind: "adapter_fidelity_report",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_adapter_fidelity_report_id",
        status_key: "local_primitive_native_io_certificate_status",
        payload_keys: LOCAL_PRIMITIVE_NATIVE_IO_ADAPTER_FIDELITY_REPORT_PAYLOAD_KEYS,
    },
];

const EXECUTION_MODE_SELECTION_REPORT_PAYLOAD_KEYS: &[&str] = &[
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
];

const COMPUTE_FLOW_EVIDENCE_PAYLOAD_KEYS: &[&str] = &[
    "selected_execution_mode",
    "execution_mode_family",
    "source_format",
    "workload_constitution_id",
    "prepared_artifact_ref",
    "prepared_artifact_fact_ref",
    "prepared_artifact_dim_ref",
    "prepared_artifact_digest",
    "prepared_artifact_fact_digest",
    "prepared_artifact_dim_digest",
    "prepared_artifact_lifecycle_status",
    "prepared_artifact_cleanup_policy",
    "prepared_artifact_reuse_eligible",
    "prepared_artifact_workspace",
    "fact_vortex_path",
    "dim_vortex_path",
    "fact_vortex_digest",
    "dim_vortex_digest",
    "computed_result_sink_requested",
    "computed_result_sink_written",
    "computed_result_sink_replay_verified",
    "computed_result_vortex_path",
    "computed_result_vortex_bytes",
    "computed_result_vortex_digest",
    "computed_result_sink_rows",
    "computed_result_sink_rows_materialized",
    "computed_result_sink_schema_summary",
    "computed_result_sink_write_micros",
    "computed_result_sink_replay_result_json",
    "native_io_certificate_id",
    "native_io_certificate_status",
    "native_io_certificate_path_id",
    "source_native_io_certificate_status",
    "output_replay_native_io_certificate_status",
    "computed_result_sink_native_io_certificate_id",
    "computed_result_sink_native_io_certificate_status",
    "result_sink_claim_gate_status",
    "result_sink_claim_gate_reason",
    "commit_state",
    "rollback_cleanup_status",
    "runtime_execution_certificate_status",
    "provider_admission_report_id",
    "vortex_first_provider_check_performed",
    "provider_admission_classification",
    "provider_kind",
    "provider_api_surface",
    "source_backed_encoded_provider_checked",
    "source_backed_encoded_provider_status",
    "operator_blocker_matrix_ref",
    "operator_execution_class_vocabulary",
    "operator_execution_class",
    "operator_admission_status",
    "operator_blocker_id",
    "operator_blocker_reason",
    "operator_encoded_native_claim_allowed",
    "operator_residual_native_used",
    "operator_temporary_materialization_used",
    "operator_unsupported_diagnostic",
    "operator_claim_boundary",
    "residual_executor",
    "residual_boundary",
    "representation_transition_summary",
    "native_io_representation_transition_order",
    "encoded_native_execution_status",
    "fusion_status",
    "fusion_blocker",
    "materialization_boundary_report_emitted",
    "materialization_boundary_rows",
    "native_io_materialization_boundary_order",
    "data_decoded",
    "data_materialized",
    "row_read",
    "arrow_converted",
    "compatibility_import_included",
    "vortex_prepare_included",
    "vortex_write_reopen_included",
    "direct_transient_execution",
    "claim_gate_status",
    "claim_gate_reason",
    "fallback_attempted",
    "external_engine_invoked",
];

const EXECUTION_CERTIFICATE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "certificate_schema_version",
    "artifact_count",
    "required_artifact_count",
    "hash_required_count",
    "machine_readable_required_count",
    "artifact_order",
    "machine_readable_certificate_surface",
    "deterministic_field_order_required",
    "certificate_evaluation_performed",
    "runtime_execution",
    "data_read",
    "data_materialized",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const NATIVE_IO_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "contract_count",
    "representation_state_count",
    "transition_example_count",
    "certificate_path_requirement_count",
    "native_io_source_sink_coverage_row_count",
    "native_io_source_sink_coverage_source_count",
    "native_io_source_sink_coverage_sink_count",
    "contract_kind_order",
    "representation_state_order",
    "transition_example_order",
    "certificate_path_order",
    "native_io_source_sink_coverage_schema_version",
    "native_io_source_sink_coverage_status",
    "native_io_source_sink_coverage_row_order",
    "native_io_source_sink_coverage_claim_gate_status",
    "native_io_source_sink_coverage_all_rows_fallback_attempted_false",
    "native_io_source_sink_coverage_all_rows_external_engine_invoked_false",
    "native_io_source_sink_coverage_all_unadmitted_rows_have_diagnostics",
    "per_path_certificate_required",
    "aggregate_certificate_not_sufficient",
    "materialization_boundary_required_for_decoded_columnar",
    "materialization_boundary_required_for_rows",
    "source_pushdown_proof_required",
    "sink_requirement_propagation_required",
    "adapter_fidelity_report_required",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const BENCHMARK_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "status",
    "benchmark_execution_implemented",
    "external_baselines",
    "scenario_count",
    "required_metric_count",
    "required_metric_order",
    "external_baseline_engine_order",
    "claim_gate_status",
    "claim_gate_correctness_evidence",
    "claim_gate_benchmark_evidence",
    "claim_gate_comparison_report",
    "claim_gate_reproducibility_evidence",
    "performance_claim_allowed",
    "fallback_execution_allowed",
];

const BENCHMARK_CLAIM_EVIDENCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "scope",
    "claim_evidence_status",
    "surface_count",
    "blocked_surface_count",
    "blocked_surface_order",
    "scenario_count",
    "required_metric_count",
    "expected_result_count",
    "result_count",
    "missing_result_count",
    "run_manifest_status",
    "comparison_report_status",
    "claim_gate_status",
    "claim_gate_benchmark_evidence",
    "measured_benchmark_result_rows_required",
    "measured_benchmark_result_rows_present",
    "benchmark_execution_performed",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const CLAIM_GATE_CLOSEOUT_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "scope",
    "p7_closeout_status",
    "claim_gate_status",
    "release_readiness_status",
    "claim_allowed",
    "production_claim_allowed",
    "public_release_claim_allowed",
    "public_package_claim_allowed",
    "comparative_benchmark_claim_allowed",
    "foundry_integration_claim_allowed",
    "allowed_claims",
    "blocked_claims",
    "out_of_scope_claims",
    "local_claim_status",
    "api_claim_status",
    "package_claim_status",
    "benchmark_claim_status",
    "integration_claim_status",
    "required_evidence_before_claims",
    "blocker_ids",
    "source_evidence_surfaces",
    "next_planned_priority",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const COMPUTE_CAPABILITY_MATRIX_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "matrix_status",
    "claim_grade_status",
    "compute_row_count",
    "operator_family_count",
    "support_status_vocabulary",
    "provider_kind_vocabulary",
    "engine_mode_vocabulary",
    "compute_row_order",
    "operator_family_order",
    "fixture_certified_count",
    "executable_uncertified_count",
    "report_only_count",
    "planned_count",
    "unsupported_count",
    "workload_certified_count",
    "production_certified_count",
    "claim_grade_compute_engine_complete",
    "performance_claim_allowed",
    "best_default_claim_allowed",
    "spark_displacement_claim_allowed",
    "production_claim_allowed",
    "all_rows_fallback_attempted_false",
    "all_rows_external_engine_invoked_false",
    "matrix_consuming_views_status",
    "matrix_consumer_views",
    "next_required_slice",
    "predicate_dtype_coverage_schema_version",
    "predicate_dtype_coverage_status",
    "predicate_dtype_coverage_scope",
    "predicate_dtype_coverage_support_status_vocabulary",
    "predicate_dtype_coverage_category_vocabulary",
    "predicate_dtype_coverage_row_count",
    "predicate_dtype_coverage_row_order",
    "predicate_dtype_coverage_claim_gate_status",
    "predicate_dtype_coverage_current_matrix_complete",
    "predicate_dtype_coverage_all_rows_have_support_status",
    "predicate_dtype_coverage_all_rows_have_evidence_gap",
    "predicate_dtype_coverage_all_rows_fallback_attempted_false",
    "predicate_dtype_coverage_all_rows_external_engine_invoked_false",
    "predicate_dtype_coverage_next_runtime_slice",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const SEMANTIC_CONFORMANCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "semantic_profile",
    "suite_status",
    "row_order",
    "semantic_dimension_count",
    "executed_fixture_count",
    "passed_fixture_count",
    "failed_fixture_count",
    "planned_fixture_count",
    "blocked_fixture_count",
    "fixture_status_vocabulary",
    "required_semantic_dimensions",
    "certification_blocker_ids",
    "semantic_failures_block_certification",
    "semantic_failures_block_benchmark_claims",
    "external_oracle_used",
    "external_engine_invoked",
    "in_memory_fixture_execution",
    "query_execution",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const INPUT_PLAN_SOURCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "source_kind",
    "adapter_kind",
    "dataset_format",
    "uri_scheme",
    "capability_status",
    "metadata_availability",
    "fidelity",
    "materialization_risk",
    "effect_level",
    "native_vortex",
    "compatibility_structured",
    "requires_credentials",
    "side_effect_free",
    "data_read",
    "data_materialized",
    "object_store_io",
    "external_effects_executed",
    "write_io",
    "execution",
    "plan_only",
    "fallback_execution_allowed",
];

const STREAMING_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "status",
    "source_kind",
    "source_zero_decode",
    "sink_kind",
    "sink_accepts_encoded",
    "sink_requires_materialization",
    "sink_preserves_metadata",
    "materialization_required",
    "best_data_work_level",
    "streaming_capability_matrix_schema_version",
    "streaming_capability_matrix_report_id",
    "streaming_capability_matrix_status",
    "streaming_capability_matrix_claim_gate_status",
    "streaming_capability_matrix_row_count",
    "streaming_capability_matrix_blocked_row_count",
    "streaming_capability_matrix_row_order",
    "streaming_capability_matrix_diagnostic_code_order",
    "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
    "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
    "external_engine_invoked",
];

const STREAMING_BATCH_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "encoded_streaming_batch_status",
    "streaming_mode",
    "source_kind",
    "source_capability",
    "sink_kind",
    "sink_capability",
    "representation",
    "zero_decode",
    "encoded_representation_preserved",
    "selection_vector_preserved",
    "materialization_required",
    "materialization_boundary",
    "bounded_parallelism",
    "bounded_memory",
    "backpressure_bounded",
    "estimated_batch_count",
    "estimated_batch_mib",
    "streams_executed",
    "tasks_executed",
    "data_read",
    "data_decoded",
    "data_materialized",
    "streaming_capability_matrix_schema_version",
    "streaming_capability_matrix_report_id",
    "streaming_capability_matrix_status",
    "streaming_capability_matrix_claim_gate_status",
    "streaming_capability_matrix_row_count",
    "streaming_capability_matrix_blocked_row_count",
    "streaming_capability_matrix_row_order",
    "streaming_capability_matrix_diagnostic_code_order",
    "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
    "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
    "fallback_execution_allowed",
    "fallback_attempted",
    "external_engine_invoked",
];

const LOCAL_COUNT_NATIVE_IO_SOURCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_count_native_io_source_kind",
    "local_count_native_io_adapter_id",
    "local_count_native_io_encoded_representation_preserved",
    "local_count_native_io_streaming_capability",
];

const LOCAL_COUNT_NATIVE_IO_SOURCE_PUSHDOWN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_count_native_io_pushdown_accepted_operations",
    "local_count_native_io_pushdown_rejected_operations",
    "local_count_native_io_pushdown_guarantee",
    "local_count_native_io_representation_transitions",
    "local_count_native_io_materialization_boundaries",
    "local_count_native_io_materializing_transitions_have_boundaries",
];

const LOCAL_COUNT_NATIVE_IO_SINK_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_count_native_io_sink_target_format",
    "local_count_native_io_sink_accepts_encoded",
];

const LOCAL_COUNT_NATIVE_IO_ADAPTER_FIDELITY_REPORT_PAYLOAD_KEYS: &[&str] =
    &["local_count_native_io_adapter_materialization_required"];

const LOCAL_PRIMITIVE_NATIVE_IO_SOURCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_primitive_native_io_source_kind",
    "local_primitive_native_io_adapter_id",
    "local_primitive_native_io_encoded_representation_preserved",
    "local_primitive_native_io_streaming_capability",
];

const LOCAL_PRIMITIVE_NATIVE_IO_SOURCE_PUSHDOWN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_primitive_native_io_pushdown_accepted_operations",
    "local_primitive_native_io_pushdown_rejected_operations",
    "local_primitive_native_io_pushdown_guarantee",
    "local_primitive_native_io_representation_transitions",
    "local_primitive_native_io_materialization_boundaries",
    "local_primitive_native_io_materializing_transitions_have_boundaries",
];

const LOCAL_PRIMITIVE_NATIVE_IO_SINK_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_primitive_native_io_sink_target_format",
    "local_primitive_native_io_sink_accepts_encoded",
    "local_primitive_native_io_sink_requires_decoded_columnar",
    "local_primitive_native_io_sink_requires_rows",
    "local_primitive_native_io_sink_supports_streaming",
];

const LOCAL_PRIMITIVE_NATIVE_IO_ADAPTER_FIDELITY_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "local_primitive_native_io_adapter_materialization_required",
    "local_primitive_native_io_adapter_encoded_representation_preserved",
];

const INPUT_ADAPTER_CAPABILITY_SNAPSHOT_KEYS: &[&str] = &[
    "adapter_count",
    "adapter_order",
    "common_structured_adapter_order",
    "critical_structured_adapter_order",
    "lakehouse_adapter_order",
    "object_store_adapter_order",
    "catalog_adapter_order",
    "effectful_adapter_order",
    "unstructured_adapter_order",
    "supported_adapter_count",
    "planned_adapter_count",
    "explicit_enablement_adapter_count",
    "native_vortex_status",
    "parquet_status",
    "arrow_ipc_status",
    "csv_status",
    "jsonl_status",
    "avro_status",
    "orc_status",
    "iceberg_compatible_status",
    "delta_compatible_status",
    "local_filesystem_status",
    "s3_compatible_status",
    "gcs_status",
    "azure_blob_adls_status",
    "http_range_status",
    "local_catalog_status",
    "hive_compatible_catalog_status",
    "unstructured_text_status",
    "plan_only",
    "execution",
    "write_io",
    "external_effects_executed",
    "fallback_execution_allowed",
];

fn typed_envelope_field_slot(key: &str) -> TypedEnvelopeFieldSlot {
    let normalized = key.to_ascii_lowercase();
    if is_policy_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::Policy
    } else if is_capability_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::CapabilitySnapshot
    } else if is_lifecycle_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::Lifecycle
    } else {
        TypedEnvelopeFieldSlot::Result
    }
}

fn is_policy_envelope_field(key: &str) -> bool {
    key.contains("fallback")
        || key.contains("side_effect")
        || key.contains("external_engine")
        || key.contains("external_runtime")
        || key.contains("network_effect")
        || key.contains("network_probe")
        || key.contains("filesystem_probe")
        || key.contains("catalog_probe")
        || key.contains("adapter_probe")
        || key.contains("dataset_probe")
        || key.contains("write_effect")
        || key.contains("write_io")
        || key.contains("unsafe_effect")
        || key.contains("destructive")
        || key.contains("policy")
        || key.ends_with("_allowed")
        || key.ends_with("_prohibited")
}

fn is_lifecycle_envelope_field(key: &str) -> bool {
    matches!(
        key,
        "mode"
            | "schema_version"
            | "protocol_id"
            | "protocol_stability"
            | "output_envelope_schema_version"
            | "transport_protocol_id"
            | "invocation_model"
            | "command_status_values"
            | "output_formats"
    ) || key.ends_with("_mode")
        || key.ends_with("_version")
        || key.ends_with("_status")
        || key.ends_with("_phase")
        || key.ends_with("_protocol")
}

fn is_capability_envelope_field(key: &str) -> bool {
    key.contains("capability")
        || key.contains("certification")
        || key.contains("feature_gate")
        || key.contains("supported")
        || key.contains("unsupported")
        || key.contains("readiness")
        || key.contains("coverage")
        || key.ends_with("_ready")
}

fn inline_report_payload_spec(command: &str) -> Option<InlineReportPayloadSpec> {
    match command {
        "execution-certificate-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "execution-certificate-plan.report",
            artifact_kind: "execution_certificate_report",
            status_key: "certificate_surface_status",
            payload_keys: EXECUTION_CERTIFICATE_REPORT_PAYLOAD_KEYS,
        }),
        "native-io-envelope-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "native-io-envelope-plan.report",
            artifact_kind: "native_io_report",
            status_key: "native_io_envelope_status",
            payload_keys: NATIVE_IO_REPORT_PAYLOAD_KEYS,
        }),
        "benchmark-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "benchmark-plan.report",
            artifact_kind: "benchmark_plan_report",
            status_key: "claim_gate_status",
            payload_keys: BENCHMARK_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        "benchmark-claim-evidence-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "benchmark-claim-evidence-plan.report",
            artifact_kind: "benchmark_claim_evidence_report",
            status_key: "claim_gate_status",
            payload_keys: BENCHMARK_CLAIM_EVIDENCE_REPORT_PAYLOAD_KEYS,
        }),
        "claim-gate-closeout" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "claim-gate-closeout.report",
            artifact_kind: "claim_gate_closeout_report",
            status_key: "claim_gate_status",
            payload_keys: CLAIM_GATE_CLOSEOUT_REPORT_PAYLOAD_KEYS,
        }),
        "compute-capability-matrix" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "compute-capability-matrix.report",
            artifact_kind: "compute_capability_matrix_report",
            status_key: "matrix_status",
            payload_keys: COMPUTE_CAPABILITY_MATRIX_REPORT_PAYLOAD_KEYS,
        }),
        "semantic-conformance-suite" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "semantic-conformance-suite.report",
            artifact_kind: "semantic_conformance_report",
            status_key: "suite_status",
            payload_keys: SEMANTIC_CONFORMANCE_REPORT_PAYLOAD_KEYS,
        }),
        "input-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "input-plan.source",
            artifact_kind: "source_report",
            status_key: "capability_status",
            payload_keys: INPUT_PLAN_SOURCE_REPORT_PAYLOAD_KEYS,
        }),
        "streaming-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "streaming-plan.materialization-boundary",
            artifact_kind: "materialization_boundary_report",
            status_key: "status",
            payload_keys: STREAMING_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        "streaming-batch-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "streaming-batch-plan.materialization-boundary",
            artifact_kind: "materialization_boundary_report",
            status_key: "encoded_streaming_batch_status",
            payload_keys: STREAMING_BATCH_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        _ => None,
    }
}

fn field_value<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|(field_key, value)| (field_key == key).then_some(value.as_str()))
}

fn inline_report_artifact(
    spec: InlineReportPayloadSpec,
    fields: &[(String, String)],
) -> OutputTypedArtifact {
    let artifact_id = field_value(fields, "report_id").unwrap_or(spec.artifact_id_fallback);
    let status = field_value(fields, spec.status_key).unwrap_or("available");
    let mut artifact = OutputTypedArtifact::new(artifact_id, spec.artifact_kind, status);
    for key in spec.payload_keys {
        if let Some(value) = field_value(fields, key) {
            artifact = artifact.with_field(*key, value);
        }
    }
    artifact
}

fn inline_report_payload(
    command: &str,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    let spec = inline_report_payload_spec(command)?;
    let artifact = inline_report_artifact(spec, fields);
    if artifact.payload.fields.is_empty() {
        None
    } else {
        Some(artifact)
    }
}

fn inline_payload_artifact_id(
    command: &str,
    artifact_kind: &str,
    field_value: Option<&str>,
) -> String {
    field_value
        .filter(|value| field_value_is_reference(value))
        .map_or_else(
            || format!("{command}.{artifact_kind}"),
            std::string::ToString::to_string,
        )
}

fn inline_prefixed_payload(
    command: &str,
    spec: InlinePrefixedPayloadSpec,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    if field_value(fields, spec.emitted_key) != Some("true") {
        return None;
    }
    let artifact_id = inline_payload_artifact_id(
        command,
        spec.artifact_kind,
        field_value(fields, spec.id_key),
    );
    let status = field_value(fields, spec.status_key).unwrap_or("available");
    let mut artifact = OutputTypedArtifact::new(artifact_id, spec.artifact_kind, status);
    for (key, value) in fields
        .iter()
        .filter(|(key, _)| key.starts_with(spec.key_prefix))
    {
        artifact = artifact.with_field(key, value);
    }
    if artifact.payload.fields.is_empty() {
        None
    } else {
        Some(artifact)
    }
}

fn inline_prefixed_payloads(
    command: &str,
    fields: &[(String, String)],
) -> Vec<OutputTypedArtifact> {
    INLINE_PREFIXED_PAYLOAD_SPECS
        .iter()
        .filter_map(|spec| inline_prefixed_payload(command, *spec, fields))
        .collect()
}

fn inline_field_subset_payload(
    command: &str,
    spec: InlineFieldSubsetPayloadSpec,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    if field_value(fields, spec.emitted_key) != Some("true") {
        return None;
    }
    let artifact_id = inline_payload_artifact_id(
        command,
        spec.artifact_kind,
        field_value(fields, spec.id_key),
    );
    let status = field_value(fields, spec.status_key).unwrap_or("available");
    let mut artifact = OutputTypedArtifact::new(artifact_id, spec.artifact_kind, status);
    for key in spec.payload_keys {
        if let Some(value) = field_value(fields, key) {
            artifact = artifact.with_field(*key, value);
        }
    }
    if artifact.payload.fields.is_empty() {
        None
    } else {
        Some(artifact)
    }
}

fn inline_field_subset_payloads(
    command: &str,
    fields: &[(String, String)],
) -> Vec<OutputTypedArtifact> {
    INLINE_FIELD_SUBSET_PAYLOAD_SPECS
        .iter()
        .filter_map(|spec| inline_field_subset_payload(command, *spec, fields))
        .collect()
}

fn inline_execution_mode_selection_payload(
    command: &str,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    field_value(fields, "selected_execution_mode")?;
    let artifact_id = inline_payload_artifact_id(
        command,
        "execution_mode_selection_report",
        field_value(fields, "execution_mode_selection_report_id"),
    );
    let status = match field_value(fields, "mode_supported") {
        Some("false") => "unsupported",
        Some("true") => "available",
        _ => "evidence_incomplete",
    };
    let mut artifact =
        OutputTypedArtifact::new(artifact_id, "execution_mode_selection_report", status);
    for key in EXECUTION_MODE_SELECTION_REPORT_PAYLOAD_KEYS {
        if let Some(value) = field_value(fields, key) {
            artifact = artifact.with_field(*key, value);
        } else {
            artifact = artifact.with_field(*key, "evidence_incomplete");
        }
    }
    Some(artifact)
}

fn inline_compute_flow_evidence_payload(
    command: &str,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    field_value(fields, "selected_execution_mode")?;
    let artifact_id = inline_payload_artifact_id(
        command,
        "compute_flow_evidence",
        field_value(fields, "compute_flow_evidence_report_id"),
    );
    let status = field_value(fields, "claim_gate_status").unwrap_or("evidence_incomplete");
    let mut artifact = OutputTypedArtifact::new(artifact_id, "compute_flow_evidence", status);
    for key in COMPUTE_FLOW_EVIDENCE_PAYLOAD_KEYS {
        artifact = artifact.with_field(
            *key,
            field_value(fields, key).unwrap_or("evidence_incomplete"),
        );
    }
    Some(artifact)
}

fn command_capability_snapshot_keys(command: &str) -> Option<&'static [&'static str]> {
    match command {
        "input-adapters" => Some(INPUT_ADAPTER_CAPABILITY_SNAPSHOT_KEYS),
        _ => None,
    }
}

fn command_capability_snapshot_fields(
    command: &str,
    fields: &[(String, String)],
) -> Vec<(String, String)> {
    let Some(keys) = command_capability_snapshot_keys(command) else {
        return Vec::new();
    };
    keys.iter()
        .filter_map(|key| {
            field_value(fields, key).map(|value| ((*key).to_string(), value.to_string()))
        })
        .collect()
}

fn apply_command_capability_snapshot_fields(
    mut envelope: OutputEnvelope,
    fields: Vec<(String, String)>,
) -> OutputEnvelope {
    for (key, value) in fields {
        if envelope
            .capability_snapshot
            .fields
            .iter()
            .any(|(existing_key, _)| existing_key == &key)
        {
            continue;
        }
        envelope = envelope.with_capability_snapshot_field(key, value);
    }
    envelope
}

fn typed_envelope_ref_slot(key: &str) -> Option<(TypedEnvelopeRefSlot, &'static str)> {
    let normalized = key.to_ascii_lowercase();
    if !is_reference_field_key(&normalized) {
        return None;
    }

    if normalized.contains("native_io_certificate") {
        Some((TypedEnvelopeRefSlot::Certificate, "native_io_certificate"))
    } else if is_execution_certificate_ref_key(&normalized) {
        Some((TypedEnvelopeRefSlot::Certificate, "execution_certificate"))
    } else if normalized.contains("execution_certificate") {
        None
    } else if normalized.contains("certificate") {
        Some((TypedEnvelopeRefSlot::Certificate, "certificate"))
    } else if normalized.contains("evidence_artifact") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "evidence_artifact"))
    } else if normalized.contains("materialization_boundary") {
        Some((
            TypedEnvelopeRefSlot::ArtifactRef,
            "materialization_boundary_report",
        ))
    } else if normalized.contains("benchmark_row") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "benchmark_row"))
    } else if normalized.contains("foundry") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "foundry_boundary_report"))
    } else if normalized.contains("source_pushdown") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "source_pushdown_report"))
    } else if normalized.contains("adapter_fidelity") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "adapter_fidelity_report"))
    } else if normalized.contains("source") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "source_report"))
    } else if normalized.contains("sink") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "sink_report"))
    } else if normalized.contains("artifact") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "artifact"))
    } else if normalized.contains("result") {
        Some((TypedEnvelopeRefSlot::ResultRef, "result"))
    } else {
        None
    }
}

fn is_execution_certificate_ref_key(key: &str) -> bool {
    [
        "execution_certificate",
        "local_primitive_execution_certificate",
    ]
    .iter()
    .any(|stem| key_matches_certificate_ref_stem(key, stem))
}

fn key_matches_certificate_ref_stem(key: &str, stem: &str) -> bool {
    let Some(prefix) = key.strip_suffix("_ref").or_else(|| key.strip_suffix("_id")) else {
        return false;
    };
    prefix == stem || prefix.ends_with(&format!("_{stem}"))
}

fn is_reference_field_key(key: &str) -> bool {
    key.ends_with("_ref")
        || key.ends_with("_refs")
        || key.ends_with("_id")
        || key.ends_with("_uri")
        || key.ends_with("_path")
}

fn field_value_is_reference(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && !matches!(
            normalized.as_str(),
            "false" | "true" | "0" | "none" | "null" | "not_performed" | "not_available"
        )
}

fn typed_ref_status(value: &str) -> &'static str {
    let normalized = value.to_ascii_lowercase();
    if normalized.contains("blocked") {
        "blocked"
    } else if normalized.contains("missing") || normalized.contains("incomplete") {
        "incomplete"
    } else {
        "available"
    }
}

fn typed_ref_id(key: &str, value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= 128 && !trimmed.contains(',') {
        trimmed.to_string()
    } else {
        key.to_string()
    }
}

fn typed_ref_uri(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let extension_candidate = std::path::Path::new(trimmed)
        .extension()
        .and_then(std::ffi::OsStr::to_str);
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || extension_candidate.is_some_and(|ext| {
            ["json", "vortex", "parquet"]
                .iter()
                .any(|known| ext.eq_ignore_ascii_case(known))
        })
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}

fn add_typed_ref_if_present(envelope: OutputEnvelope, key: &str, value: &str) -> OutputEnvelope {
    let Some((slot, kind)) = typed_envelope_ref_slot(key) else {
        return envelope;
    };
    if !field_value_is_reference(value) {
        return envelope;
    }

    let mut reference =
        OutputTypedRef::new(typed_ref_id(key, value), kind, typed_ref_status(value));
    if let Some(uri) = typed_ref_uri(value) {
        reference = reference.with_uri(uri);
    }

    match slot {
        TypedEnvelopeRefSlot::ResultRef => envelope.with_result_ref(reference),
        TypedEnvelopeRefSlot::ArtifactRef => envelope.with_artifact_ref(reference),
        TypedEnvelopeRefSlot::Certificate => envelope.with_certificate(reference),
    }
}

pub(crate) fn apply_typed_envelope_field(
    envelope: OutputEnvelope,
    key: String,
    value: String,
) -> OutputEnvelope {
    let envelope = add_typed_ref_if_present(envelope, &key, &value);
    match typed_envelope_field_slot(&key) {
        TypedEnvelopeFieldSlot::Result => envelope.with_result_field(key, value),
        TypedEnvelopeFieldSlot::Policy => envelope
            .with_policy_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
        TypedEnvelopeFieldSlot::Lifecycle => envelope
            .with_lifecycle_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
        TypedEnvelopeFieldSlot::CapabilitySnapshot => envelope
            .with_capability_snapshot_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
    }
}

pub(crate) fn apply_typed_envelope_fields(
    envelope: OutputEnvelope,
    command: &str,
    fields: Vec<(String, String)>,
) -> OutputEnvelope {
    let inline_report = inline_report_payload(command, &fields);
    let inline_prefixed_payloads = inline_prefixed_payloads(command, &fields);
    let inline_field_subset_payloads = inline_field_subset_payloads(command, &fields);
    let inline_execution_mode_selection = inline_execution_mode_selection_payload(command, &fields);
    let inline_compute_flow_evidence = inline_compute_flow_evidence_payload(command, &fields);
    let command_capability_snapshot_fields = command_capability_snapshot_fields(command, &fields);
    let mut envelope = envelope;
    for (key, value) in fields {
        envelope = apply_typed_envelope_field(envelope, key, value);
    }
    envelope =
        apply_command_capability_snapshot_fields(envelope, command_capability_snapshot_fields);
    if let Some(artifact) = inline_report {
        envelope = envelope.with_artifact(artifact);
    }
    for artifact in inline_prefixed_payloads {
        envelope = envelope.with_artifact(artifact);
    }
    for artifact in inline_field_subset_payloads {
        envelope = envelope.with_artifact(artifact);
    }
    if let Some(artifact) = inline_execution_mode_selection {
        envelope = envelope.with_artifact(artifact);
    }
    if let Some(artifact) = inline_compute_flow_evidence {
        envelope = envelope.with_artifact(artifact);
    }
    envelope
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_ref_fields_attach_typed_certificate_and_legacy_field() {
        let envelope = apply_typed_envelope_field(
            OutputEnvelope::success("test", "ok", "ok"),
            "execution_certificate_ref".to_string(),
            "cert.execution.local".to_string(),
        );

        assert_eq!(envelope.certificates.len(), 1);
        assert_eq!(envelope.certificates[0].id, "cert.execution.local");
        assert_eq!(envelope.certificates[0].kind, "execution_certificate");
        assert_eq!(envelope.certificates[0].status, "available");
        assert_eq!(
            envelope.fields,
            vec![(
                "execution_certificate_ref".to_string(),
                "cert.execution.local".to_string()
            )]
        );
    }

    #[test]
    fn execution_certificate_classifier_ignores_related_non_certificate_refs() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("test", "ok", "ok"),
            "test",
            vec![
                (
                    "execution_certificate_input_ref".to_string(),
                    "inputs/local.vortex".to_string(),
                ),
                (
                    "execution_certificate_output_ref".to_string(),
                    "outputs/local.vortex".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_fixture_id".to_string(),
                    "vortex-local-count-where-struct-five".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_id".to_string(),
                    "execution.local.fixture".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.certificates.len(), 1);
        assert_eq!(envelope.certificates[0].id, "execution.local.fixture");
        assert_eq!(envelope.certificates[0].kind, "execution_certificate");
        assert_eq!(envelope.fields.len(), 4);
    }

    #[test]
    fn artifact_ref_fields_attach_typed_artifact_ref() {
        let envelope = apply_typed_envelope_field(
            OutputEnvelope::success("test", "ok", "ok"),
            "materialization_boundary_report_ref".to_string(),
            "artifacts/materialization.json".to_string(),
        );

        assert_eq!(envelope.artifact_refs.len(), 1);
        assert_eq!(
            envelope.artifact_refs[0].kind,
            "materialization_boundary_report"
        );
        assert_eq!(
            envelope.artifact_refs[0].uri.as_deref(),
            Some("artifacts/materialization.json")
        );
    }

    #[test]
    fn native_io_subreport_ref_fields_attach_specific_artifact_refs() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("test", "ok", "ok"),
            "test",
            vec![
                (
                    "source_pushdown_report_ref".to_string(),
                    "artifacts/source-pushdown.json".to_string(),
                ),
                (
                    "adapter_fidelity_report_ref".to_string(),
                    "artifacts/adapter-fidelity.json".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifact_refs.len(), 2);
        assert!(envelope.artifact_refs.iter().any(|reference| {
            reference.kind == "source_pushdown_report"
                && reference.uri.as_deref() == Some("artifacts/source-pushdown.json")
        }));
        assert!(envelope.artifact_refs.iter().any(|reference| {
            reference.kind == "adapter_fidelity_report"
                && reference.uri.as_deref() == Some("artifacts/adapter-fidelity.json")
        }));
    }

    #[test]
    fn command_fields_attach_inline_execution_certificate_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("execution-certificate-plan", "ok", "ok"),
            "execution-certificate-plan",
            vec![
                ("mode".to_string(), "execution_certificate_plan".to_string()),
                (
                    "schema_version".to_string(),
                    "shardloom.execution_certificate_evidence_surface.v1".to_string(),
                ),
                (
                    "report_id".to_string(),
                    "cg16.execution-certificate-evidence-surface".to_string(),
                ),
                (
                    "certificate_surface_status".to_string(),
                    "report_only_planned".to_string(),
                ),
                ("artifact_count".to_string(), "6".to_string()),
                (
                    "machine_readable_certificate_surface".to_string(),
                    "true".to_string(),
                ),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "cg16.execution-certificate-evidence-surface"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "execution_certificate_report"
        );
        assert_eq!(envelope.artifacts[0].status, "report_only_planned");
        assert!(
            envelope.artifacts[0]
                .payload
                .fields
                .contains(&("artifact_count".to_string(), "6".to_string()))
        );
        assert!(envelope.fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_benchmark_claim_evidence_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("benchmark-claim-evidence-plan", "ok", "ok"),
            "benchmark-claim-evidence-plan",
            vec![
                ("mode".to_string(), "benchmark_claim_evidence".to_string()),
                (
                    "report_id".to_string(),
                    "cg6.benchmark_claim_evidence.aggregate".to_string(),
                ),
                (
                    "claim_gate_status".to_string(),
                    "evidence_missing".to_string(),
                ),
                (
                    "measured_benchmark_result_rows_present".to_string(),
                    "false".to_string(),
                ),
                ("fallback_attempted".to_string(), "false".to_string()),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "benchmark_claim_evidence_report"
        );
        assert_eq!(envelope.artifacts[0].status, "evidence_missing");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "measured_benchmark_result_rows_present".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_execution_mode_selection_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("traditional-analytics-run", "ok", "ok"),
            "traditional-analytics-run",
            execution_mode_selection_test_fields(),
        );

        let artifact = envelope
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_kind == "execution_mode_selection_report")
            .expect("execution mode selection artifact");
        assert_eq!(
            artifact.artifact_id,
            "traditional-analytics-run.execution_mode_selection_report"
        );
        assert_eq!(artifact.status, "available");
        assert!(artifact.payload.fields.contains(&(
            "selected_execution_mode".to_string(),
            "compatibility_import_certified".to_string()
        )));
        assert!(
            artifact
                .payload
                .fields
                .contains(&("fallback_attempted".to_string(), "false".to_string()))
        );
        let compute_flow = envelope
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_kind == "compute_flow_evidence")
            .expect("compute flow evidence artifact");
        assert_eq!(
            compute_flow.artifact_id,
            "traditional-analytics-run.compute_flow_evidence"
        );
        assert_eq!(compute_flow.status, "not_claim_grade");
        assert!(compute_flow.payload.fields.contains(&(
            "fact_vortex_path".to_string(),
            "evidence_incomplete".to_string()
        )));
    }

    fn execution_mode_selection_test_fields() -> Vec<(String, String)> {
        vec![
            (
                "execution_mode_selection_schema_version".to_string(),
                "shardloom.execution_mode_selection_report.v1".to_string(),
            ),
            ("requested_execution_mode".to_string(), "auto".to_string()),
            (
                "selected_execution_mode".to_string(),
                "compatibility_import_certified".to_string(),
            ),
            (
                "execution_mode".to_string(),
                "compatibility_import_certified".to_string(),
            ),
            (
                "mode_selection_reason".to_string(),
                "auto_selected_certified_ingest_stage_requested".to_string(),
            ),
            (
                "execution_mode_family".to_string(),
                "compatibility".to_string(),
            ),
            ("source_format".to_string(), "csv".to_string()),
            (
                "workload_constitution_id".to_string(),
                "local_vortex_analytics_v1".to_string(),
            ),
            (
                "compatibility_import_included".to_string(),
                "true".to_string(),
            ),
            ("vortex_prepare_included".to_string(), "true".to_string()),
            (
                "vortex_write_reopen_included".to_string(),
                "true".to_string(),
            ),
            (
                "direct_transient_execution".to_string(),
                "false".to_string(),
            ),
            (
                "vortex_native_claim_allowed".to_string(),
                "false".to_string(),
            ),
            ("certification_requested".to_string(), "true".to_string()),
            ("result_sink_requested".to_string(), "true".to_string()),
            (
                "prepared_artifact_available".to_string(),
                "false".to_string(),
            ),
            (
                "native_vortex_provider_available".to_string(),
                "false".to_string(),
            ),
            ("mode_supported".to_string(), "true".to_string()),
            ("support_status".to_string(), "supported".to_string()),
            (
                "unsupported_diagnostic_code".to_string(),
                "none".to_string(),
            ),
            ("blocker_id".to_string(), "none".to_string()),
            ("required_future_evidence".to_string(), "none".to_string()),
            (
                "claim_gate_status".to_string(),
                "not_claim_grade".to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                "compatibility_import_certified_ingest_stage_not_pure_compute".to_string(),
            ),
            ("fallback_attempted".to_string(), "false".to_string()),
            ("external_engine_invoked".to_string(), "false".to_string()),
        ]
    }

    fn emitted_runtime_certificate_envelope() -> OutputEnvelope {
        apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-run", "ok", "ok"),
            "vortex-run",
            vec![
                (
                    "local_primitive_native_io_certificate_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "local_primitive_native_io_certificate_id".to_string(),
                    "native-io.local.fixture".to_string(),
                ),
                (
                    "local_primitive_native_io_certificate_status".to_string(),
                    "certified".to_string(),
                ),
                (
                    "local_primitive_native_io_source_kind".to_string(),
                    "vortex_file".to_string(),
                ),
                (
                    "local_primitive_native_io_adapter_id".to_string(),
                    "vortex-local".to_string(),
                ),
                (
                    "local_primitive_native_io_pushdown_guarantee".to_string(),
                    "metadata_only".to_string(),
                ),
                (
                    "local_primitive_native_io_sink_target_format".to_string(),
                    "vortex".to_string(),
                ),
                (
                    "local_primitive_native_io_adapter_materialization_required".to_string(),
                    "false".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_id".to_string(),
                    "execution.local.fixture".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_status".to_string(),
                    "certified".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_fixture_id".to_string(),
                    "vortex-local-count-where-struct-five".to_string(),
                ),
            ],
        )
    }

    #[test]
    fn command_fields_attach_inline_emitted_runtime_certificate_payloads() {
        let envelope = emitted_runtime_certificate_envelope();

        assert_eq!(envelope.artifacts.len(), 6);
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "native-io.local.fixture"
                && artifact.artifact_kind == "native_io_certificate"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_source_kind".to_string(),
                    "vortex_file".to_string(),
                ))
        }));
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "execution.local.fixture"
                && artifact.artifact_kind == "execution_certificate"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_execution_certificate_fixture_id".to_string(),
                    "vortex-local-count-where-struct-five".to_string(),
                ))
        }));
    }

    #[test]
    fn command_fields_attach_inline_emitted_native_io_subreports() {
        let envelope = emitted_runtime_certificate_envelope();

        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "vortex-run.source_report"
                && artifact.artifact_kind == "source_report"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_adapter_id".to_string(),
                    "vortex-local".to_string(),
                ))
        }));
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "vortex-run.source_pushdown_report"
                && artifact.artifact_kind == "source_pushdown_report"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_pushdown_guarantee".to_string(),
                    "metadata_only".to_string(),
                ))
        }));
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "vortex-run.sink_report"
                && artifact.artifact_kind == "sink_report"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_sink_target_format".to_string(),
                    "vortex".to_string(),
                ))
        }));
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "vortex-run.adapter_fidelity_report"
                && artifact.artifact_kind == "adapter_fidelity_report"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_adapter_materialization_required".to_string(),
                    "false".to_string(),
                ))
        }));
    }

    #[test]
    fn command_fields_skip_inline_runtime_certificate_payload_when_not_emitted() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-count", "ok", "ok"),
            "vortex-count",
            vec![
                (
                    "execution_certificate_emitted".to_string(),
                    "false".to_string(),
                ),
                (
                    "execution_certificate_status".to_string(),
                    "evidence_unavailable".to_string(),
                ),
            ],
        );

        assert!(envelope.artifacts.is_empty());
        assert_eq!(
            envelope.lifecycle.fields,
            vec![(
                "execution_certificate_status".to_string(),
                "evidence_unavailable".to_string()
            )]
        );
    }

    #[test]
    fn command_fields_attach_inline_streaming_runtime_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-count", "ok", "ok"),
            "vortex-count",
            vec![
                (
                    "streaming_batch_runtime_report_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "streaming_batch_runtime_status".to_string(),
                    "executed".to_string(),
                ),
                (
                    "streaming_batch_runtime_representation".to_string(),
                    "vortex_encoded".to_string(),
                ),
                (
                    "streaming_batch_runtime_zero_decode".to_string(),
                    "preserved".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "vortex-count.streaming_batch_runtime_report"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "streaming_batch_runtime_report"
        );
        assert_eq!(envelope.artifacts[0].status, "executed");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "streaming_batch_runtime_zero_decode".to_string(),
            "preserved".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_materialization_boundary_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("streaming-batch-plan", "ok", "ok"),
            "streaming-batch-plan",
            vec![
                ("mode".to_string(), "streaming_batch_plan".to_string()),
                (
                    "encoded_streaming_batch_status".to_string(),
                    "requires_materialization".to_string(),
                ),
                (
                    "materialization_boundary".to_string(),
                    "full_materialization_boundary".to_string(),
                ),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "streaming-batch-plan.materialization-boundary"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "materialization_boundary_report"
        );
        assert_eq!(envelope.artifacts[0].status, "requires_materialization");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "materialization_boundary".to_string(),
            "full_materialization_boundary".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_input_source_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("input-plan", "ok", "ok"),
            "input-plan",
            vec![
                ("mode".to_string(), "input_plan".to_string()),
                ("source_kind".to_string(), "parquet".to_string()),
                ("adapter_kind".to_string(), "structured_file".to_string()),
                ("dataset_format".to_string(), "parquet".to_string()),
                ("uri_scheme".to_string(), "s3".to_string()),
                ("capability_status".to_string(), "planned".to_string()),
                ("data_read".to_string(), "false".to_string()),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(envelope.artifacts[0].artifact_id, "input-plan.source");
        assert_eq!(envelope.artifacts[0].artifact_kind, "source_report");
        assert_eq!(envelope.artifacts[0].status, "planned");
        assert!(
            envelope.artifacts[0]
                .payload
                .fields
                .contains(&("source_kind".to_string(), "parquet".to_string()))
        );
    }

    #[test]
    fn command_fields_enrich_input_adapter_capability_snapshot() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("input-adapters", "ok", "ok"),
            "input-adapters",
            vec![
                ("mode".to_string(), "input_adapters".to_string()),
                ("adapter_count".to_string(), "32".to_string()),
                ("supported_adapter_count".to_string(), "0".to_string()),
                ("planned_adapter_count".to_string(), "22".to_string()),
                ("native_vortex_status".to_string(), "planned".to_string()),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert!(
            envelope
                .capability_snapshot
                .fields
                .contains(&("adapter_count".to_string(), "32".to_string()))
        );
        assert!(
            envelope
                .capability_snapshot
                .fields
                .contains(&("native_vortex_status".to_string(), "planned".to_string()))
        );
        assert_eq!(
            envelope
                .capability_snapshot
                .fields
                .iter()
                .filter(|(key, _)| key == "supported_adapter_count")
                .count(),
            1
        );
    }
}
