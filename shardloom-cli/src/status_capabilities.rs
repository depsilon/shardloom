//! Status and capability-discovery CLI handlers.
//!
//! This is the first physical command-family handler split for Priority 3.9.
//! It keeps behavior identical to the old `main.rs` match arms while routing
//! output through the shared typed-envelope renderer.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{
    ArchitectureRuntimeClaimGateReport, CapabilityCertificationReport,
    CapabilityCertificationStatus, CommandStatus, EngineCapabilities, EngineCapabilityMatrixReport,
    MaterializationPolicyReport, OutputFormat, PhysicalOperatorExecutionLevel,
    PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorPlan, ShardLoomError,
    SqlDataFramePlannerReadinessMatrix, WorldClassSufficiencyDimensionKind,
    WorldClassSufficiencyReport, boundedness_vocabulary, engine_mode_vocabulary,
    output_mode_vocabulary, plan_global_architecture_runtime_claim_gate,
    plan_materialization_policy_report, plan_world_class_sufficiency, update_mode_vocabulary,
};
use shardloom_exec::StreamingCapabilityMatrixReport;
use shardloom_vortex::{
    vortex_encoded_count_local_guard_discovery_report,
    vortex_encoded_count_physical_kernel_discovery_report,
    vortex_encoded_predicate_evaluation_discovery_report,
    vortex_selection_vector_filter_kernel_discovery_report,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
    engine_runtime_planning::append_streaming_capability_matrix_summary_fields,
};

const WORKFLOW_OPERATION_NAMES: &str = concat!(
    "profile,collect,from_pandas,from_arrow_table,from_arrow_ipc,",
    "to_pandas,to_arrow,to_arrow_table,to_arrow_ipc,to_numpy,to_python_objects,",
    "with_column,group_by,agg,sort,limit,write_vortex,write_parquet,sql,",
    "sql_parse,sql_bind,sql_plan,sql_execute,join,aggregate,window,",
    "schema_contract,schema,describe_schema,validate_schema,data_quality,",
    "data_quality_summary,quarantine,preview,display,object_store_read,",
    "fallback_engine"
);
const WORKFLOW_BLOCKER_IDS: &str = concat!(
    "cg21.workflow.profile.runtime_profile_unsupported,",
    "cg21.workflow.collect.materialization_unsupported,",
    "cg21.workflow.from_pandas.materialized_input_unsupported,",
    "cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported,",
    "cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported,",
    "cg21.workflow.to_pandas.decoded_dataframe_unsupported,",
    "cg21.workflow.to_arrow.decoded_columnar_unsupported,",
    "cg21.workflow.to_arrow_table.decoded_table_unsupported,",
    "cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported,",
    "cg21.workflow.to_numpy.python_array_unsupported,",
    "cg21.workflow.to_python_objects.object_materialization_unsupported,",
    "cg21.workflow.with_column.expression_unsupported,",
    "cg21.workflow.group_by.operator_unsupported,",
    "cg21.workflow.agg.operator_unsupported,",
    "cg21.workflow.sort.operator_unsupported,",
    "cg21.workflow.limit.execution_uncertified,",
    "cg21.workflow.write_vortex.write_policy_unsupported,",
    "cg21.workflow.write_parquet.compatibility_export_unsupported,",
    "cg21.workflow.sql.frontend_unsupported,",
    "cg21.workflow.sql.parse_unsupported,",
    "cg21.workflow.sql.bind_unsupported,",
    "cg21.workflow.sql.plan_unsupported,",
    "cg21.workflow.sql.execute_unsupported,",
    "cg21.workflow.join.operator_unsupported,",
    "cg21.workflow.aggregate.operator_unsupported,",
    "cg21.workflow.window.operator_unsupported,",
    "cg21.workflow.schema_contract.enforcement_unsupported,",
    "cg21.workflow.schema.discovery_unsupported,",
    "cg21.workflow.describe_schema.report_unsupported,",
    "cg21.workflow.validate_schema.validation_unsupported,",
    "cg21.workflow.data_quality.checks_unsupported,",
    "cg21.workflow.data_quality_summary.report_unsupported,",
    "cg21.workflow.quarantine.output_unsupported,",
    "cg21.workflow.preview.materialization_unsupported,",
    "cg21.workflow.display.rich_display_unsupported,",
    "cg21.workflow.object_store_read.runtime_unsupported,",
    "cg21.workflow.fallback_engine.no_fallback_policy"
);
const WORKFLOW_REQUIRED_EVIDENCE: &str = "execution_certificate,native_io_certificate,operator_capability_matrix,semantic_conformance_suite,sql_parser,binder,write_intent,rest_api_contract,decoded_columnar_boundary,python_object_boundary,schema_metadata_report,data_quality_report,notebook_display_boundary,object_store_capability_policy,credential_policy,no_fallback_policy";
const WORKFLOW_SUGGESTED_NEXT_ACTION: &str = "Use workflow-unsupported-plan for method-specific blocker details before requesting execution.";
const REMOTE_API_BLOCKER_IDS: &str = concat!(
    "cg23.remote_api.plan_preview.unsupported_operator,",
    "cg23.remote_api.remote_object_store.unsupported,",
    "cg23.remote_api.lifecycle.uncertified_blocked,",
    "cg23.remote_api.data_plane.materialization_boundary_required"
);
const REMOTE_API_REQUIRED_EVIDENCE: &str = "openapi_contract,asyncapi_contract,execution_certificate,native_io_certificate,security_governance_policy,data_plane_fidelity_report";
const REMOTE_API_SUGGESTED_NEXT_ACTION: &str = "Use rest-api-contract-plan and rest-api-plan-preview for scenario-specific blockers before enabling remote execution.";
const COMPUTE_CAPABILITY_COMMAND: &str = "compute-capability-matrix";
const COMPUTE_CAPABILITY_USAGE: &str = "usage: shardloom compute-capability-matrix";
const GLOBAL_ARCHITECTURE_GATE_COMMAND: &str = "global-architecture-gate";
const GLOBAL_ARCHITECTURE_GATE_USAGE: &str = "usage: shardloom global-architecture-gate";
const COMPUTE_SUPPORT_STATUS_VOCABULARY: &str = "unsupported,planned,report_only,executable_uncertified,fixture_certified,workload_certified,production_certified";
const COMPUTE_PROVIDER_KIND_VOCABULARY: &str = "shardloom_kernel,vortex_array_kernel,vortex_scan,vortex_source,vortex_sink,compatibility_boundary,external_baseline_only";
const COMPUTE_ENGINE_MODE_VOCABULARY: &str = "batch,live,hybrid,auto";
const COMPUTE_EXECUTION_MODE_VOCABULARY: &str = "compatibility_import_certified,prepared_vortex,native_vortex,direct_compatibility_transient,auto";
const COMPUTE_OPERATOR_EXECUTION_CLASS_VOCABULARY: &str =
    "encoded_native,residual_native,materialized_temporary,unsupported";
const NATIVE_VORTEX_ADMISSION_SCHEMA_VERSION: &str = "shardloom.native_vortex_admission.v1";
const NATIVE_UNSUPPORTED_COVERAGE_SCHEMA_VERSION: &str = "shardloom.native_unsupported_coverage.v1";
const NATIVE_UNSUPPORTED_COVERAGE_CATEGORY_VOCABULARY: &str = "source,sink,operator,workload";
const PREDICATE_DTYPE_COVERAGE_SCHEMA_VERSION: &str = "shardloom.predicate_dtype_coverage.v1";
const PREDICATE_DTYPE_COVERAGE_SUPPORT_STATUS_VOCABULARY: &str =
    "unsupported,fixture_needed,executable_uncertified,fixture_certified,claim_grade";
const PREDICATE_DTYPE_COVERAGE_CATEGORY_VOCABULARY: &str =
    "predicate,dtype,null_semantics,nested_shape,statistics";

const COMPUTE_ROWS: &[ComputeCapabilityRow] = &[
    ComputeCapabilityRow {
        id: "local_vortex_count",
        surface: "count_all",
        family: "aggregates",
        support_status: "fixture_certified",
        engine_mode: "batch",
        provider_kind: "vortex_scan",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "metadata_or_encoded_count_no_row_materialization",
        memory_spill_requirement: "streaming_constant_memory_no_spill",
        correctness_refs: "cg5.local_vortex_count,query_primitive_correctness",
        benchmark_refs: "vortex-count-benchmark.local_fixture_smoke",
        execution_certificate_refs: "certificates/cg16/local-vortex-count/execution.json",
        native_io_refs: "certificates/cg19/local-vortex-count/native-io.json",
        unsupported_diagnostic_code: "none",
        blocker_id: "none",
        required_future_evidence: "claim_grade_benchmark_rows",
    },
    ComputeCapabilityRow {
        id: "local_vortex_filtered_count",
        surface: "count_where",
        family: "predicates",
        support_status: "executable_uncertified",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "selection_vector_no_row_materialization",
        memory_spill_requirement: "bounded_selection_vector_no_spill",
        correctness_refs: "query_primitive_correctness.filtered_count",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.filtered_count.certification_incomplete",
        required_future_evidence: "benchmark_row,execution_certificate,native_io_certificate",
    },
    ComputeCapabilityRow {
        id: "local_vortex_projection",
        surface: "project_columns",
        family: "projection",
        support_status: "executable_uncertified",
        engine_mode: "batch",
        provider_kind: "vortex_array_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "column_projection_no_row_materialization",
        memory_spill_requirement: "bounded_column_refs_no_spill",
        correctness_refs: "query_primitive_correctness.projection",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.projection.certification_incomplete",
        required_future_evidence: "benchmark_row,execution_certificate,native_io_certificate",
    },
    ComputeCapabilityRow {
        id: "local_vortex_filter_project",
        surface: "filter_project",
        family: "filter_project_fusion",
        support_status: "executable_uncertified",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "selection_vector_plus_projection_no_row_materialization",
        memory_spill_requirement: "bounded_selection_vector_no_spill",
        correctness_refs: "query_primitive_correctness.filter_project",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.filter_project.certification_incomplete",
        required_future_evidence: "benchmark_row,execution_certificate,native_io_certificate",
    },
    ComputeCapabilityRow {
        id: "prepared_encoded_filter",
        surface: "prepared_encoded_filter",
        family: "predicates",
        support_status: "fixture_certified",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "encoded_predicate_no_row_materialization",
        memory_spill_requirement: "bounded_batch_no_spill",
        correctness_refs: "prepared_encoded_correctness_fixture",
        benchmark_refs: "source_backed_benchmark_row_required",
        execution_certificate_refs: "prepared_encoded_execution_certificate",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.prepared_encoded_filter.source_backed_rows_missing",
        required_future_evidence: "measured_source_backed_benchmark_row,reproducibility_manifest",
    },
    ComputeCapabilityRow {
        id: "reader_backed_dictionary_filter",
        surface: "reader_backed_dictionary_filter",
        family: "predicates",
        support_status: "executable_uncertified",
        engine_mode: "batch",
        provider_kind: "vortex_source",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "reader_chunk_kernel_input_no_full_decode",
        memory_spill_requirement: "reader_chunk_bounded_no_spill",
        correctness_refs: "reader_backed_dictionary_fixture",
        benchmark_refs: "source_backed_benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.reader_backed_dictionary_filter.measurement_missing",
        required_future_evidence: "measured_source_backed_benchmark_row,provider_version",
    },
    ComputeCapabilityRow {
        id: "compatibility_csv_import",
        surface: "csv_compatibility_import",
        family: "sources",
        support_status: "executable_uncertified",
        engine_mode: "batch",
        provider_kind: "compatibility_boundary",
        semantic_profile: "compatibility_boundary",
        materialization_decode_requirement: "compatibility_import_to_native_vortex",
        memory_spill_requirement: "bounded_local_import_no_spill_claim",
        correctness_refs: "traditional_analytics_csv_smoke",
        benchmark_refs: "traditional_analytics_taxonomy_row",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required_after_native_vortex_stage",
        unsupported_diagnostic_code: "none",
        blocker_id: "p74.compute.compatibility_import.certification_incomplete",
        required_future_evidence: "adapter_fidelity_report,native_io_certificate,benchmark_row",
    },
    ComputeCapabilityRow {
        id: "direct_compatibility_transient",
        surface: "direct_compatibility_transient_query",
        family: "compatibility_transient",
        support_status: "unsupported",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "direct_transient_executor_missing",
        memory_spill_requirement: "unsupported_until_transient_executor_exists",
        correctness_refs: "none",
        benchmark_refs: "none",
        execution_certificate_refs: "none",
        native_io_refs: "not_vortex_native",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT",
        blocker_id: "p75.direct_transient.executor_missing",
        required_future_evidence: "shardloom_native_transient_executor,direct_mode_certificate,correctness_fixtures,no_fallback_evidence",
    },
    ComputeCapabilityRow {
        id: "vortex_sink_write",
        surface: "write_vortex",
        family: "sink_write_operators",
        support_status: "report_only",
        engine_mode: "batch",
        provider_kind: "vortex_sink",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "sink_requirement_known_before_execution",
        memory_spill_requirement: "write_buffer_policy_required",
        correctness_refs: "write_intent_report",
        benchmark_refs: "write_benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        unsupported_diagnostic_code: "SL_NOT_IMPLEMENTED",
        blocker_id: "p74.compute.vortex_sink.write_execution_missing",
        required_future_evidence: "sink_execution,commit_recovery,artifact_replay",
    },
    ComputeCapabilityRow {
        id: "grouped_aggregate",
        surface: "group_by_aggregate",
        family: "grouped_aggregates",
        support_status: "planned",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "group_state_materialization_policy_required",
        memory_spill_requirement: "hash_group_state_spill_required",
        correctness_refs: "semantic_fixture_required",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        unsupported_diagnostic_code: "SL_NOT_IMPLEMENTED",
        blocker_id: "cg21.workflow.aggregate.operator_unsupported",
        required_future_evidence: "operator_capability,semantic_fixture,memory_spill_declaration",
    },
    ComputeCapabilityRow {
        id: "join",
        surface: "join",
        family: "joins",
        support_status: "planned",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "join_build_probe_materialization_policy_required",
        memory_spill_requirement: "join_state_spill_required",
        correctness_refs: "semantic_fixture_required",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        unsupported_diagnostic_code: "SL_NOT_IMPLEMENTED",
        blocker_id: "cg21.workflow.join.operator_unsupported",
        required_future_evidence: "join_operator_capability,semantic_fixture,memory_spill_declaration",
    },
    ComputeCapabilityRow {
        id: "window_row_number",
        surface: "row_number_window",
        family: "window_functions",
        support_status: "planned",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "window_frame_materialization_policy_required",
        memory_spill_requirement: "sort_partition_spill_required",
        correctness_refs: "semantic_fixture_required",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        unsupported_diagnostic_code: "SL_NOT_IMPLEMENTED",
        blocker_id: "cg21.workflow.window.operator_unsupported",
        required_future_evidence: "window_operator_capability,sort_capability,semantic_fixture",
    },
    ComputeCapabilityRow {
        id: "sql_frontend",
        surface: "sql_parse_bind_plan_execute",
        family: "sql_frontend",
        support_status: "unsupported",
        engine_mode: "batch",
        provider_kind: "shardloom_kernel",
        semantic_profile: "ShardLoomNative",
        materialization_decode_requirement: "logical_plan_lowering_required",
        memory_spill_requirement: "depends_on_lowered_operator_family",
        correctness_refs: "semantic_fixture_required",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL",
        blocker_id: "cg21.workflow.sql.frontend_unsupported",
        required_future_evidence: "sql_parser,binder,semantic_profile,operator_capability_matrix",
    },
];

const OPERATOR_FAMILY_ROWS: &[OperatorFamilyCoverageRow] = &[
    OperatorFamilyCoverageRow {
        id: "scalar_expressions",
        support_status: "planned",
        next_evidence: "expression_registry,semantic_fixtures",
    },
    OperatorFamilyCoverageRow {
        id: "predicates",
        support_status: "fixture_certified",
        next_evidence: "source_backed_measured_rows,semantic_edge_cases",
    },
    OperatorFamilyCoverageRow {
        id: "projection",
        support_status: "executable_uncertified",
        next_evidence: "benchmark_rows,execution_certificates",
    },
    OperatorFamilyCoverageRow {
        id: "filter_project_fusion",
        support_status: "executable_uncertified",
        next_evidence: "benchmark_rows,execution_certificates",
    },
    OperatorFamilyCoverageRow {
        id: "aggregates",
        support_status: "fixture_certified",
        next_evidence: "grouped_aggregate_semantics,claim_grade_benchmarks",
    },
    OperatorFamilyCoverageRow {
        id: "grouped_aggregates",
        support_status: "planned",
        next_evidence: "hash_group_state,memory_spill,semantic_fixtures",
    },
    OperatorFamilyCoverageRow {
        id: "approx_sketch_aggregates",
        support_status: "report_only",
        next_evidence: "sketch_semantics,error_bounds,benchmarks",
    },
    OperatorFamilyCoverageRow {
        id: "sort_topn_limit",
        support_status: "planned",
        next_evidence: "ordering_semantics,memory_spill,benchmarks",
    },
    OperatorFamilyCoverageRow {
        id: "joins",
        support_status: "planned",
        next_evidence: "join_null_semantics,build_probe_memory,benchmarks",
    },
    OperatorFamilyCoverageRow {
        id: "semi_anti_joins",
        support_status: "planned",
        next_evidence: "join_operator_capability,semantic_fixtures",
    },
    OperatorFamilyCoverageRow {
        id: "window_functions",
        support_status: "planned",
        next_evidence: "window_frame_semantics,sort_spill,benchmarks",
    },
    OperatorFamilyCoverageRow {
        id: "set_operations",
        support_status: "planned",
        next_evidence: "distinct_semantics,memory_spill",
    },
    OperatorFamilyCoverageRow {
        id: "nested_extension_type_operations",
        support_status: "planned",
        next_evidence: "nested_equality,extension_dtype_semantics",
    },
    OperatorFamilyCoverageRow {
        id: "sink_write_operators",
        support_status: "report_only",
        next_evidence: "write_execution,commit_recovery,replay_verification",
    },
];

const NATIVE_VORTEX_ADMISSION_LANES: &[NativeVortexAdmissionLane] = &[NativeVortexAdmissionLane {
    id: "local_vortex_count_scalar",
    source_surface: "local_vortex_file_scan",
    operator_surface: "count_all",
    sink_surface: "typed_scalar_result",
    admission_status: "admitted_fixture_certified",
    support_status: "fixture_certified",
    execution_mode: "native_vortex",
    provider_kind: "vortex_scan",
    provider_api_surface: "VortexFile::scan,ScanBuilder::into_array_iter",
    provider_crate: "vortex",
    provider_version: "0.70",
    feature_gate: "vortex-encoded-read-spike",
    shardloom_admission_policy: "local_fixture_scan_count_only",
    compute_row_ref: "compute_row.local_vortex_count",
    benchmark_ref: "vortex-count-benchmark.local_fixture_smoke",
    correctness_refs: "cg5.local_vortex_count,query_primitive_correctness",
    execution_certificate_refs: "certificates/cg16/local-vortex-count/execution.json",
    native_io_refs: "certificates/cg19/local-vortex-count/native-io.json",
    materialization_decode_refs: "native_vortex_source_to_scalar_count_result",
    policy_refs: "fallback_attempted=false,external_engine_invoked=false",
    required_future_evidence: "claim_grade_benchmark_rows,broad_source_sink_operator_coverage",
    claim_gate_status: "fixture_smoke_only",
    claim_boundary: "local_count_all_fixture_smoke_only_not_universal_native_vortex",
    residual_executor: "none",
}];

const NATIVE_UNSUPPORTED_COVERAGE_ROWS: &[NativeUnsupportedCoverageRow] = &[
    NativeUnsupportedCoverageRow {
        id: "native_source_object_store_range",
        category: "source",
        surface: "object_store_range_read",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SOURCE",
        blocker_id: "gar0002.native.source.object_store_range",
        required_future_evidence: "object_store_request_planner,range_read_certificate,native_io_certificate",
        source_refs: "docs/architecture/object-store-request-planner.md,docs/architecture/vortex-upstream-alignment-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_source_table_catalog",
        category: "source",
        surface: "table_catalog_snapshot_read",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_TABLE_CATALOG_SOURCE",
        blocker_id: "gar0002.native.source.table_catalog",
        required_future_evidence: "table_catalog_metadata_read,namespace_policy,native_io_certificate",
        source_refs: "docs/architecture/table-intelligence-layer.md,docs/architecture/universal-input-contract.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_source_streaming_events",
        category: "source",
        surface: "streaming_event_source",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_STREAM_SOURCE",
        blocker_id: "gar0002.native.source.streaming_events",
        required_future_evidence: "boundedness_policy,checkpoint_contract,execution_certificate",
        source_refs: "docs/architecture/dynamic-work-shaping.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_source_unstructured_media",
        category: "source",
        surface: "unstructured_document_media_source",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_UNSTRUCTURED_MEDIA_SOURCE",
        blocker_id: "gar0002.native.source.unstructured_media",
        required_future_evidence: "media_decoder_policy,materialization_boundary,semantic_fixture",
        source_refs: "docs/architecture/global-architecture-review.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_sink_object_store_write",
        category: "sink",
        surface: "object_store_native_write",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SINK",
        blocker_id: "gar0002.native.sink.object_store_write",
        required_future_evidence: "object_store_commit_protocol,retry_checkpoint_evidence,native_io_certificate",
        source_refs: "docs/architecture/object-store-request-planner.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_sink_table_catalog_commit",
        category: "sink",
        surface: "table_catalog_commit",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_TABLE_COMMIT",
        blocker_id: "gar0002.native.sink.table_catalog_commit",
        required_future_evidence: "commit_protocol,manifest_finalization,delete_tombstone_semantics",
        source_refs: "docs/architecture/table-intelligence-layer.md,docs/architecture/object-store-request-planner.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_sink_streaming_events",
        category: "sink",
        surface: "streaming_event_sink",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_STREAM_SINK",
        blocker_id: "gar0002.native.sink.streaming_events",
        required_future_evidence: "delivery_semantics,checkpoint_recovery,effect_budget_policy",
        source_refs: "docs/architecture/effect-budget-plan.md,docs/architecture/dynamic-work-shaping.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_sink_compatibility_export",
        category: "sink",
        surface: "compatibility_export_writer",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_COMPATIBILITY_EXPORT_SINK",
        blocker_id: "gar0002.native.sink.compatibility_export",
        required_future_evidence: "adapter_fidelity_report,materialization_boundary,write_certificate",
        source_refs: "docs/architecture/universal-input-contract.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_scalar_expressions",
        category: "operator",
        surface: "scalar_expression_registry",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_SCALAR_EXPRESSIONS",
        blocker_id: "gar0002.native.operator.scalar_expressions",
        required_future_evidence: "expression_registry,semantic_fixtures,execution_certificate",
        source_refs: "docs/architecture/capability-certification-sequencing.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_grouped_aggregates",
        category: "operator",
        surface: "grouped_aggregate",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_GROUPED_AGGREGATE",
        blocker_id: "cg21.workflow.aggregate.operator_unsupported",
        required_future_evidence: "group_state_memory_policy,semantic_fixture,benchmark_row",
        source_refs: "docs/architecture/compute-engine-flow-reference.md,docs/architecture/capability-certification-sequencing.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_sort_topn_limit",
        category: "operator",
        surface: "sort_topn_limit",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_SORT_TOPN_LIMIT",
        blocker_id: "gar0002.native.operator.sort_topn_limit",
        required_future_evidence: "ordering_semantics,spill_policy,benchmark_row",
        source_refs: "docs/architecture/compute-engine-flow-reference.md,docs/architecture/dynamic-work-shaping.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_joins",
        category: "operator",
        surface: "join",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_JOIN",
        blocker_id: "cg21.workflow.join.operator_unsupported",
        required_future_evidence: "join_null_semantics,build_probe_memory_policy,benchmark_row",
        source_refs: "docs/architecture/compute-engine-flow-reference.md,docs/architecture/correctness-differential-harness.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_window_functions",
        category: "operator",
        surface: "window_functions",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_WINDOW",
        blocker_id: "cg21.workflow.window.operator_unsupported",
        required_future_evidence: "window_frame_semantics,sort_spill_policy,benchmark_row",
        source_refs: "docs/architecture/compute-engine-flow-reference.md,docs/architecture/correctness-differential-harness.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_approx_sketch",
        category: "operator",
        surface: "approx_sketch_aggregates",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_APPROX_SKETCH",
        blocker_id: "gar0002.native.operator.approx_sketch",
        required_future_evidence: "error_bounds,sketch_seed_metadata,semantic_fixture",
        source_refs: "docs/architecture/capability-certification-sequencing.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_set_operations",
        category: "operator",
        surface: "set_operations",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_SET_OPERATIONS",
        blocker_id: "gar0002.native.operator.set_operations",
        required_future_evidence: "distinct_semantics,memory_spill_policy,semantic_fixture",
        source_refs: "docs/architecture/capability-certification-sequencing.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_operator_nested_extension_types",
        category: "operator",
        surface: "nested_extension_type_operations",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_EXTENSION_TYPE_OPERATION",
        blocker_id: "gar0002.native.operator.nested_extension_types",
        required_future_evidence: "nested_equality,extension_dtype_semantics,semantic_fixture",
        source_refs: "docs/architecture/vortex-upstream-alignment-hardening.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_sql_dataframe",
        category: "workload",
        surface: "sql_dataframe_frontend",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_SQL_DATAFRAME_RUNTIME",
        blocker_id: "cg21.workflow.sql.frontend_unsupported",
        required_future_evidence: "sql_parser,binder,planner,dataframe_api_semantics",
        source_refs: "docs/architecture/global-architecture-review.md,docs/architecture/rfc-coverage-followthrough.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_live_hybrid_runtime",
        category: "workload",
        surface: "live_hybrid_engine_mode",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_LIVE_HYBRID_RUNTIME",
        blocker_id: "gar0002.native.workload.live_hybrid_runtime",
        required_future_evidence: "state_lifecycle,checkpoint_recovery,boundedness_contract",
        source_refs: "docs/architecture/dynamic-work-shaping.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_distributed_object_store",
        category: "workload",
        surface: "distributed_object_store_execution",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_DISTRIBUTED_OBJECT_STORE_RUNTIME",
        blocker_id: "gar0002.native.workload.distributed_object_store",
        required_future_evidence: "range_scheduler,coalescing_policy,retry_checkpoint_certificate",
        source_refs: "docs/architecture/object-store-request-planner.md,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_rest_foundry_remote",
        category: "workload",
        surface: "rest_foundry_remote_runtime",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_REST_FOUNDRY_RUNTIME",
        blocker_id: "gar0002.native.workload.rest_foundry_remote",
        required_future_evidence: "rest_lifecycle,foundry_package_proof,remote_policy_certificate",
        source_refs: "docs/api/shardloom-openapi-v1.yaml,docs/architecture/global-architecture-review.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_udf_external_effects",
        category: "workload",
        surface: "udf_llm_embedding_external_effects",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_EXTERNAL_EFFECT_RUNTIME",
        blocker_id: "gar0002.native.workload.external_effects",
        required_future_evidence: "sandbox_runtime,credential_policy,effect_budget_certificate",
        source_refs: "docs/architecture/effect-budget-plan.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
    NativeUnsupportedCoverageRow {
        id: "native_workload_best_default_claim",
        category: "workload",
        surface: "best_default_claim_grade_runtime",
        support_status: "unsupported",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_BEST_DEFAULT_CLAIM",
        blocker_id: "gar0002.native.workload.best_default_claim",
        required_future_evidence: "workload_scoped_benchmark_evidence,release_gate,public_claim_review",
        source_refs: "docs/architecture/benchmark-competitive-claim-evidence.md,docs/architecture/operational-evidence-policy-hardening.md",
    },
];

const PREDICATE_DTYPE_COVERAGE_ROWS: &[PredicateDtypeCoverageRow] = &[
    PredicateDtypeCoverageRow {
        id: "predicate_i64_range",
        category: "predicate",
        family: "range_comparison",
        surface: "i64_min_max_pruning_and_native_filter",
        support_status: "fixture_certified",
        runtime_surface: "metadata_pruning,prepared_vortex,native_vortex",
        statistics_required: "row_count,min_value,max_value,null_count",
        fixture_status: "local_fixture_present",
        correctness_refs: "query_primitive_correctness.filtered_count,traditional_analytics.partition_pruning",
        benchmark_refs: "traditional_analytics.partition_pruning,vortex-count-where.fixture_smoke",
        execution_certificate_refs: "fixture_execution_certificate_required_for_claim_grade",
        native_io_refs: "native_io_certificate_required_for_source_bound_data",
        materialization_decode_refs: "metadata_pruning_or_encoded_filter_no_full_materialization",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.range_claim_grade_evidence_missing",
        required_future_evidence: "claim_grade_range_fixture_matrix,benchmark_rows,native_io_certificate",
        claim_gate_status: "fixture_smoke_only",
        claim_boundary: "scoped i64 range/equality fixture coverage, not broad predicate coverage",
    },
    PredicateDtypeCoverageRow {
        id: "predicate_i64_equality",
        category: "predicate",
        family: "equality_comparison",
        surface: "i64_eq_ne_constant_or_minmax",
        support_status: "executable_uncertified",
        runtime_surface: "prepared_vortex,native_vortex",
        statistics_required: "row_count,min_value,max_value,constant_value_indicator",
        fixture_status: "fixture_expansion_required",
        correctness_refs: "query_primitive_correctness.filtered_count",
        benchmark_refs: "benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "encoded_predicate_or_residual_native_filter",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.equality_fixture_matrix_missing",
        required_future_evidence: "constant_segment_fixture,dictionary_absence_fixture,benchmark_row",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "i64 equality execution may run where admitted, but broad pruning is not claim-grade",
    },
    PredicateDtypeCoverageRow {
        id: "predicate_string_dictionary",
        category: "predicate",
        family: "dictionary_membership",
        surface: "utf8_dictionary_equality_or_absence",
        support_status: "fixture_needed",
        runtime_surface: "reader_backed_encoded,prepared_vortex",
        statistics_required: "dictionary_values,row_count,null_count",
        fixture_status: "fixture_needed",
        correctness_refs: "reader_backed_dictionary_fixture_required",
        benchmark_refs: "source_backed_benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "dictionary_encoded_no_full_string_materialization_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.dictionary_membership_fixture_missing",
        required_future_evidence: "dictionary_absence_fixture,string_null_fixture,source_backed_benchmark_row",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "dictionary/string predicate readiness only; no broad string predicate claim",
    },
    PredicateDtypeCoverageRow {
        id: "predicate_boolean_counts",
        category: "predicate",
        family: "boolean_statistics",
        surface: "true_false_count_metadata_answer",
        support_status: "fixture_needed",
        runtime_surface: "metadata_only",
        statistics_required: "row_count,true_count,false_count,null_count",
        fixture_status: "fixture_needed",
        correctness_refs: "boolean_count_fixture_required",
        benchmark_refs: "metadata_only_benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "metadata_only_answer_no_data_read",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.boolean_statistics_fixture_missing",
        required_future_evidence: "true_false_count_exactness_fixture,missing_stat_diagnostic,benchmark_row",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "boolean metadata-answer contract only until exact-stat fixtures exist",
    },
    PredicateDtypeCoverageRow {
        id: "predicate_compound_or_not",
        category: "predicate",
        family: "compound_predicates",
        surface: "and_or_not_predicate_pruning",
        support_status: "unsupported",
        runtime_surface: "unsupported",
        statistics_required: "per_child_conservative_proof,three_valued_logic",
        fixture_status: "blocked",
        correctness_refs: "semantic_fixture_required",
        benchmark_refs: "none",
        execution_certificate_refs: "none",
        native_io_refs: "none",
        materialization_decode_refs: "unsupported_no_decode_or_materialization",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_COMPOUND_PREDICATE_PRUNING",
        blocker_id: "gar0006a.compound_predicate_pruning_unsupported",
        required_future_evidence: "and_or_not_semantic_fixtures,null_truth_table,deterministic_diagnostic",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "compound predicates remain unsupported for metadata pruning",
    },
    PredicateDtypeCoverageRow {
        id: "dtype_int64",
        category: "dtype",
        family: "numeric_primitives",
        surface: "int64_stats_and_encoded_values",
        support_status: "fixture_certified",
        runtime_surface: "metadata_pruning,prepared_vortex,native_vortex",
        statistics_required: "row_count,min_value,max_value,null_count",
        fixture_status: "local_fixture_present",
        correctness_refs: "query_primitive_correctness,local_vortex_struct_fixture",
        benchmark_refs: "vortex-count-benchmark.local_fixture_smoke,traditional_analytics.partition_pruning",
        execution_certificate_refs: "certificates/cg16/local-vortex-count/execution.json",
        native_io_refs: "certificates/cg19/local-vortex-count/native-io.json",
        materialization_decode_refs: "metadata_or_encoded_count_no_row_materialization",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.int64_claim_grade_evidence_missing",
        required_future_evidence: "broader_numeric_fixture_matrix,source_bound_native_io_certificate",
        claim_gate_status: "fixture_smoke_only",
        claim_boundary: "int64 local fixture coverage only, not production DType coverage",
    },
    PredicateDtypeCoverageRow {
        id: "dtype_utf8_dictionary",
        category: "dtype",
        family: "string_dictionary",
        surface: "utf8_dictionary_encoded_values",
        support_status: "executable_uncertified",
        runtime_surface: "reader_backed_encoded,prepared_vortex",
        statistics_required: "dictionary_values,row_count,null_count",
        fixture_status: "fixture_expansion_required",
        correctness_refs: "reader_backed_dictionary_fixture",
        benchmark_refs: "source_backed_benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "dictionary_encoded_boundary_no_full_decode_required",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.utf8_dictionary_claim_grade_missing",
        required_future_evidence: "string_dictionary_fixture_matrix,benchmark_row,certificate_refs",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "utf8 dictionary support is not broad string or nested text support",
    },
    PredicateDtypeCoverageRow {
        id: "dtype_decimal_timestamp",
        category: "dtype",
        family: "temporal_decimal",
        surface: "timestamp_decimal_stats_pruning",
        support_status: "fixture_needed",
        runtime_surface: "metadata_pruning_planned",
        statistics_required: "logical_type,min_value,max_value,timezone_or_scale_policy,null_count",
        fixture_status: "fixture_needed",
        correctness_refs: "timestamp_decimal_fixture_required",
        benchmark_refs: "dirty_timestamp_cleanup_coverage,benchmark_row_required",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "metadata_pruning_requires_logical_type_exactness",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.temporal_decimal_exactness_missing",
        required_future_evidence: "timezone_semantics,decimal_scale_semantics,malformed_value_fixture",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "temporal/decimal pruning remains fixture-needed",
    },
    PredicateDtypeCoverageRow {
        id: "null_all_null_segments",
        category: "null_semantics",
        family: "all_null_segments",
        surface: "is_null_is_not_null_metadata_answer",
        support_status: "fixture_needed",
        runtime_surface: "metadata_only,metadata_pruning",
        statistics_required: "row_count,null_count",
        fixture_status: "fixture_needed",
        correctness_refs: "all_null_segment_fixture_required",
        benchmark_refs: "null_heavy_aggregate_coverage_row",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "metadata_only_null_answer_no_data_read",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.all_null_fixture_missing",
        required_future_evidence: "all_null_fixture,is_null_is_not_null_truth_table,benchmark_row",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "null semantics are explicit but all-null pruning is not claim-grade",
    },
    PredicateDtypeCoverageRow {
        id: "null_mixed_segments",
        category: "null_semantics",
        family: "mixed_null_segments",
        surface: "mixed_null_comparison_truth_table",
        support_status: "fixture_needed",
        runtime_surface: "prepared_vortex,native_vortex",
        statistics_required: "row_count,null_count,min_value,max_value",
        fixture_status: "fixture_needed",
        correctness_refs: "mixed_null_truth_table_fixture_required",
        benchmark_refs: "null_heavy_aggregate_coverage_row",
        execution_certificate_refs: "execution_certificate_required",
        native_io_refs: "native_io_certificate_required",
        materialization_decode_refs: "conservative_native_filter_when_metadata_proof_missing",
        unsupported_diagnostic_code: "none",
        blocker_id: "gar0006a.mixed_null_truth_table_missing",
        required_future_evidence: "mixed_null_fixture,three_valued_logic_policy,benchmark_row",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "mixed-null paths must not prune without conservative proof",
    },
    PredicateDtypeCoverageRow {
        id: "nested_field_pruning",
        category: "nested_shape",
        family: "nested_struct_list_map",
        surface: "nested_json_or_struct_field_predicate",
        support_status: "unsupported",
        runtime_surface: "unsupported",
        statistics_required: "nested_field_path_stats,parent_child_presence,definition_repetition_policy",
        fixture_status: "blocked",
        correctness_refs: "nested_json_fixture_required",
        benchmark_refs: "nested_json_field_scan_coverage_only",
        execution_certificate_refs: "none",
        native_io_refs: "none",
        materialization_decode_refs: "unsupported_no_nested_decode_or_materialization",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_NESTED_FIELD_PRUNING",
        blocker_id: "gar0006a.nested_field_pruning_unsupported",
        required_future_evidence: "nested_path_stats,struct_list_map_semantics,deterministic_diagnostic",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "nested benchmark fixture coverage is not native nested pruning support",
    },
    PredicateDtypeCoverageRow {
        id: "statistics_missing_or_inexact",
        category: "statistics",
        family: "missing_or_inexact_stats",
        surface: "metadata_only_answer_when_stats_absent",
        support_status: "unsupported",
        runtime_surface: "unsupported_for_metadata_only",
        statistics_required: "exact_required_stat",
        fixture_status: "blocked",
        correctness_refs: "missing_stats_fixture_required",
        benchmark_refs: "none",
        execution_certificate_refs: "none",
        native_io_refs: "none",
        materialization_decode_refs: "unsupported_metadata_answer_no_fallback",
        unsupported_diagnostic_code: "SL_UNSUPPORTED_METADATA_ONLY_STATISTICS",
        blocker_id: "gar0006a.missing_or_inexact_statistics",
        required_future_evidence: "missing_stats_diagnostic_fixture,exactness_policy,native_execution_fallback_to_shardloom_path",
        claim_gate_status: "not_claim_grade",
        claim_boundary: "missing stats never prove absence or authorize fallback execution",
    },
];

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

pub(crate) fn handle_compute_capability_matrix(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            COMPUTE_CAPABILITY_COMMAND,
            format,
            "compute capability matrix failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected compute-capability-matrix argument: {extra}; {COMPUTE_CAPABILITY_USAGE}"
            )),
        );
    }

    emit(
        COMPUTE_CAPABILITY_COMMAND,
        format,
        CommandStatus::Success,
        "compute capability coverage matrix".to_string(),
        compute_capability_matrix_human_text(),
        vec![],
        compute_capability_matrix_fields(),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_global_architecture_gate(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            GLOBAL_ARCHITECTURE_GATE_COMMAND,
            format,
            "global architecture runtime claim gate failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected global-architecture-gate argument: {extra}; {GLOBAL_ARCHITECTURE_GATE_USAGE}"
            )),
        );
    }

    let report = plan_global_architecture_runtime_claim_gate();
    emit(
        GLOBAL_ARCHITECTURE_GATE_COMMAND,
        format,
        CommandStatus::Success,
        "global architecture runtime claim gate".to_string(),
        report.to_human_text(),
        vec![],
        global_architecture_gate_fields(&report),
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
    let streaming_matrix = StreamingCapabilityMatrixReport::gar0013_current();
    append_streaming_capability_matrix_summary_fields(&mut fields, &streaming_matrix);
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
    push_count_field(&mut fields, "workflow_operation_count", 37);
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

struct ComputeCapabilityRow {
    id: &'static str,
    surface: &'static str,
    family: &'static str,
    support_status: &'static str,
    engine_mode: &'static str,
    provider_kind: &'static str,
    semantic_profile: &'static str,
    materialization_decode_requirement: &'static str,
    memory_spill_requirement: &'static str,
    correctness_refs: &'static str,
    benchmark_refs: &'static str,
    execution_certificate_refs: &'static str,
    native_io_refs: &'static str,
    unsupported_diagnostic_code: &'static str,
    blocker_id: &'static str,
    required_future_evidence: &'static str,
}

struct OperatorFamilyCoverageRow {
    id: &'static str,
    support_status: &'static str,
    next_evidence: &'static str,
}

struct NativeVortexAdmissionLane {
    id: &'static str,
    source_surface: &'static str,
    operator_surface: &'static str,
    sink_surface: &'static str,
    admission_status: &'static str,
    support_status: &'static str,
    execution_mode: &'static str,
    provider_kind: &'static str,
    provider_api_surface: &'static str,
    provider_crate: &'static str,
    provider_version: &'static str,
    feature_gate: &'static str,
    shardloom_admission_policy: &'static str,
    compute_row_ref: &'static str,
    benchmark_ref: &'static str,
    correctness_refs: &'static str,
    execution_certificate_refs: &'static str,
    native_io_refs: &'static str,
    materialization_decode_refs: &'static str,
    policy_refs: &'static str,
    required_future_evidence: &'static str,
    claim_gate_status: &'static str,
    claim_boundary: &'static str,
    residual_executor: &'static str,
}

struct NativeUnsupportedCoverageRow {
    id: &'static str,
    category: &'static str,
    surface: &'static str,
    support_status: &'static str,
    unsupported_diagnostic_code: &'static str,
    blocker_id: &'static str,
    required_future_evidence: &'static str,
    source_refs: &'static str,
}

struct PredicateDtypeCoverageRow {
    id: &'static str,
    category: &'static str,
    family: &'static str,
    surface: &'static str,
    support_status: &'static str,
    runtime_surface: &'static str,
    statistics_required: &'static str,
    fixture_status: &'static str,
    correctness_refs: &'static str,
    benchmark_refs: &'static str,
    execution_certificate_refs: &'static str,
    native_io_refs: &'static str,
    materialization_decode_refs: &'static str,
    unsupported_diagnostic_code: &'static str,
    blocker_id: &'static str,
    required_future_evidence: &'static str,
    claim_gate_status: &'static str,
    claim_boundary: &'static str,
}

fn compute_capability_matrix_human_text() -> String {
    let materialization_report = plan_materialization_policy_report();
    format!(
        "compute capability coverage matrix\nrows: {}\nfamilies: {}\nnative Vortex admission lanes: {}\nnative unsupported coverage rows: {}\npredicate/DType coverage rows: {}\nmaterialization policy rows: {}\nclaim grade: blocked for broad claims\nfallback execution: disabled\nruntime execution: false\nside effects: none",
        COMPUTE_ROWS.len(),
        OPERATOR_FAMILY_ROWS.len(),
        NATIVE_VORTEX_ADMISSION_LANES.len(),
        NATIVE_UNSUPPORTED_COVERAGE_ROWS.len(),
        PREDICATE_DTYPE_COVERAGE_ROWS.len(),
        materialization_report.rows.len()
    )
}

#[allow(clippy::too_many_lines)]
fn compute_capability_matrix_fields() -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "compute_capability_matrix");
    push_field(
        &mut fields,
        "schema_version",
        "shardloom.compute_capability_matrix.v1",
    );
    push_field(&mut fields, "report_id", "p74.compute_capability_matrix");
    push_field(&mut fields, "matrix_status", "report_only");
    push_field(&mut fields, "claim_grade_status", "evidence_incomplete");
    push_count_field(&mut fields, "compute_row_count", COMPUTE_ROWS.len());
    push_count_field(
        &mut fields,
        "operator_family_count",
        OPERATOR_FAMILY_ROWS.len(),
    );
    push_field(
        &mut fields,
        "support_status_vocabulary",
        COMPUTE_SUPPORT_STATUS_VOCABULARY,
    );
    push_field(
        &mut fields,
        "provider_kind_vocabulary",
        COMPUTE_PROVIDER_KIND_VOCABULARY,
    );
    push_field(
        &mut fields,
        "engine_mode_vocabulary",
        COMPUTE_ENGINE_MODE_VOCABULARY,
    );
    push_field(
        &mut fields,
        "execution_mode_vocabulary",
        COMPUTE_EXECUTION_MODE_VOCABULARY,
    );
    push_field(
        &mut fields,
        "operator_execution_class_vocabulary",
        COMPUTE_OPERATOR_EXECUTION_CLASS_VOCABULARY,
    );
    push_bool_field(&mut fields, "mode_aware_rows_present", true);
    push_bool_field(
        &mut fields,
        "direct_transient_unsupported_parity_present",
        true,
    );
    push_field(&mut fields, "compute_row_order", &compute_row_order());
    push_field(
        &mut fields,
        "operator_family_order",
        &operator_family_order(),
    );
    push_count_field(
        &mut fields,
        "fixture_certified_count",
        compute_support_status_count("fixture_certified"),
    );
    push_count_field(
        &mut fields,
        "executable_uncertified_count",
        compute_support_status_count("executable_uncertified"),
    );
    push_count_field(
        &mut fields,
        "report_only_count",
        compute_support_status_count("report_only"),
    );
    push_count_field(
        &mut fields,
        "planned_count",
        compute_support_status_count("planned"),
    );
    push_count_field(
        &mut fields,
        "unsupported_count",
        compute_support_status_count("unsupported"),
    );
    push_count_field(
        &mut fields,
        "workload_certified_count",
        compute_support_status_count("workload_certified"),
    );
    push_count_field(
        &mut fields,
        "production_certified_count",
        compute_support_status_count("production_certified"),
    );
    push_bool_field(&mut fields, "claim_grade_compute_engine_complete", false);
    push_bool_field(&mut fields, "performance_claim_allowed", false);
    push_bool_field(&mut fields, "best_default_claim_allowed", false);
    push_bool_field(&mut fields, "spark_displacement_claim_allowed", false);
    push_bool_field(&mut fields, "production_claim_allowed", false);
    push_bool_field(&mut fields, "all_rows_fallback_attempted_false", true);
    push_bool_field(&mut fields, "all_rows_external_engine_invoked_false", true);
    push_field(
        &mut fields,
        "matrix_consuming_views_status",
        "planned_alignment",
    );
    push_field(
        &mut fields,
        "matrix_consumer_views",
        "capabilities operators,capabilities workflow,capabilities engines,benchmark-plan,workload-certification-dossier,rest-api-plan-preview",
    );
    push_field(
        &mut fields,
        "next_required_slice",
        "P7.4.2 semantic conformance and unsupported API parity",
    );
    append_native_vortex_admission_fields(&mut fields);
    append_native_unsupported_coverage_fields(&mut fields);
    append_predicate_dtype_coverage_fields(&mut fields);
    append_materialization_policy_fields(&mut fields, &plan_materialization_policy_report());
    for row in COMPUTE_ROWS {
        append_compute_capability_row_fields(&mut fields, row);
    }
    for row in OPERATOR_FAMILY_ROWS {
        append_operator_family_row_fields(&mut fields, row);
    }
    push_bool_field(&mut fields, "plan_only", true);
    push_bool_field(&mut fields, "runtime_execution", false);
    push_bool_field(&mut fields, "query_execution", false);
    push_bool_field(&mut fields, "data_read", false);
    push_bool_field(&mut fields, "data_materialized", false);
    push_bool_field(&mut fields, "write_io", false);
    push_bool_field(&mut fields, "object_store_io", false);
    push_bool_field(&mut fields, "network_probe", false);
    push_bool_field(&mut fields, "catalog_probe", false);
    push_bool_field(&mut fields, "external_engine_invoked", false);
    push_bool_field(&mut fields, "external_effects_executed", false);
    push_bool_field(&mut fields, "fallback_execution_allowed", false);
    push_bool_field(&mut fields, "fallback_attempted", false);
    push_bool_field(&mut fields, "no_runtime", true);
    push_bool_field(&mut fields, "no_fallback", true);
    push_bool_field(&mut fields, "no_effects", true);
    fields
}

fn global_architecture_gate_fields(
    report: &ArchitectureRuntimeClaimGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(
        &mut fields,
        "mode",
        "global_architecture_runtime_claim_gate",
    );
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "docs_ref", report.docs_ref);
    push_field(&mut fields, "source_refs", report.source_refs);
    push_field(
        &mut fields,
        "support_status_vocabulary",
        report.support_status_vocabulary,
    );
    push_field(&mut fields, "claim_gate_status", report.claim_gate_status);
    push_count_field(&mut fields, "row_count", report.rows.len());
    push_field(&mut fields, "row_order", &report.row_order().join(","));
    push_field(
        &mut fields,
        "claim_families",
        &report.claim_families().join(","),
    );
    push_field(
        &mut fields,
        "existing_gate_refs",
        &report.existing_gate_refs.join(","),
    );
    push_field(
        &mut fields,
        "required_gate_refs",
        &report.required_gate_refs.join(","),
    );
    push_field(
        &mut fields,
        "unsupported_diagnostic_codes",
        &report.unsupported_diagnostic_codes().join(","),
    );
    push_field(&mut fields, "blocker_ids", &report.blocker_ids().join(","));
    push_field(
        &mut fields,
        "required_evidence",
        &report.required_evidence().join("|"),
    );
    push_bool_field(
        &mut fields,
        "release_gate_required",
        report.release_gate_required,
    );
    push_bool_field(
        &mut fields,
        "runtime_claim_allowed",
        report.runtime_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "distributed_runtime_claim_allowed",
        report.distributed_runtime_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "object_store_runtime_claim_allowed",
        report.object_store_runtime_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "lakehouse_runtime_claim_allowed",
        report.lakehouse_runtime_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "public_claim_allowed",
        report.public_claim_allowed,
    );
    append_global_architecture_no_effect_fields(&mut fields, report);
    push_bool_field(
        &mut fields,
        "all_rows_side_effect_free",
        report.all_rows_side_effect_free(),
    );
    push_bool_field(
        &mut fields,
        "all_rows_not_claim_grade",
        report.all_rows_not_claim_grade(),
    );
    push_bool_field(
        &mut fields,
        "all_runtime_claims_blocked",
        report.all_runtime_claims_blocked(),
    );
    push_bool_field(
        &mut fields,
        "deterministic_diagnostics_present",
        report.deterministic_diagnostics_present(),
    );
    push_field(&mut fields, "execution", "not_performed");
    push_bool_field(&mut fields, "plan_only", true);
    fields
}

fn append_global_architecture_no_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ArchitectureRuntimeClaimGateReport,
) {
    push_bool_field(
        fields,
        "coordinator_worker_start_allowed",
        report.coordinator_worker_start_allowed,
    );
    push_bool_field(
        fields,
        "task_execution_allowed",
        report.task_execution_allowed,
    );
    push_bool_field(
        fields,
        "credential_resolution_allowed",
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        "object_store_io_allowed",
        report.object_store_io_allowed,
    );
    push_bool_field(
        fields,
        "table_catalog_io_allowed",
        report.table_catalog_io_allowed,
    );
    push_bool_field(
        fields,
        "lakehouse_commit_allowed",
        report.lakehouse_commit_allowed,
    );
    push_bool_field(fields, "data_read_allowed", report.data_read_allowed);
    push_bool_field(fields, "write_io_allowed", report.write_io_allowed);
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
}

fn append_native_vortex_admission_fields(fields: &mut Vec<(String, String)>) {
    append_native_vortex_admission_summary_fields(fields);
    for lane in NATIVE_VORTEX_ADMISSION_LANES {
        append_native_vortex_admission_lane_fields(fields, lane);
    }
}

fn append_native_vortex_admission_summary_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "native_vortex_admission_schema_version",
        NATIVE_VORTEX_ADMISSION_SCHEMA_VERSION,
    );
    push_field(
        fields,
        "native_vortex_admission_status",
        "scoped_fixture_lane_admitted",
    );
    push_field(
        fields,
        "native_vortex_admission_scope",
        "current_certified_local_lanes",
    );
    push_count_field(
        fields,
        "native_vortex_admission_lane_count",
        NATIVE_VORTEX_ADMISSION_LANES.len(),
    );
    push_field(
        fields,
        "native_vortex_admission_lane_order",
        &native_vortex_admission_lane_order(),
    );
    push_field(
        fields,
        "native_vortex_admission_claim_gate_status",
        "fixture_smoke_only",
    );
    push_bool_field(
        fields,
        "native_vortex_admission_universal_coverage_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "native_vortex_admission_all_lanes_fallback_attempted_false",
        true,
    );
    push_bool_field(
        fields,
        "native_vortex_admission_all_lanes_external_engine_invoked_false",
        true,
    );
    push_field(
        fields,
        "native_vortex_admission_policy_refs",
        "docs/architecture/vortex-public-api-inventory.md,docs/architecture/operational-evidence-policy-hardening.md",
    );
    push_field(
        fields,
        "native_vortex_admission_claim_boundary",
        "admitted lanes are exact fixture-scoped evidence, not universal native Vortex support",
    );
}

fn append_native_unsupported_coverage_fields(fields: &mut Vec<(String, String)>) {
    append_native_unsupported_coverage_summary_fields(fields);
    append_native_unsupported_coverage_invariant_fields(fields);
    for row in NATIVE_UNSUPPORTED_COVERAGE_ROWS {
        append_native_unsupported_coverage_row_fields(fields, row);
    }
}

fn append_native_unsupported_coverage_summary_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "native_unsupported_coverage_schema_version",
        NATIVE_UNSUPPORTED_COVERAGE_SCHEMA_VERSION,
    );
    push_field(
        fields,
        "native_unsupported_coverage_status",
        "complete_for_current_matrix",
    );
    push_field(
        fields,
        "native_unsupported_coverage_scope",
        "current_source_sink_operator_workload_matrix",
    );
    push_field(
        fields,
        "native_unsupported_coverage_category_vocabulary",
        NATIVE_UNSUPPORTED_COVERAGE_CATEGORY_VOCABULARY,
    );
    push_count_field(
        fields,
        "native_unsupported_coverage_row_count",
        NATIVE_UNSUPPORTED_COVERAGE_ROWS.len(),
    );
    push_count_field(
        fields,
        "native_unsupported_coverage_source_count",
        native_unsupported_category_count("source"),
    );
    push_count_field(
        fields,
        "native_unsupported_coverage_sink_count",
        native_unsupported_category_count("sink"),
    );
    push_count_field(
        fields,
        "native_unsupported_coverage_operator_count",
        native_unsupported_category_count("operator"),
    );
    push_count_field(
        fields,
        "native_unsupported_coverage_workload_count",
        native_unsupported_category_count("workload"),
    );
    push_field(
        fields,
        "native_unsupported_coverage_row_order",
        &native_unsupported_row_order(),
    );
}

fn append_native_unsupported_coverage_invariant_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "native_unsupported_coverage_claim_gate_status",
        "not_claim_grade",
    );
    push_bool_field(
        fields,
        "native_unsupported_coverage_current_matrix_complete",
        true,
    );
    push_bool_field(
        fields,
        "native_unsupported_coverage_all_rows_claim_gate_not_grade",
        true,
    );
    push_bool_field(
        fields,
        "native_unsupported_coverage_all_rows_fallback_attempted_false",
        true,
    );
    push_bool_field(
        fields,
        "native_unsupported_coverage_all_rows_external_engine_invoked_false",
        true,
    );
    push_count_field(
        fields,
        "unadmitted_compute_row_count",
        unadmitted_compute_row_count(),
    );
    push_count_field(
        fields,
        "unadmitted_compute_rows_with_diagnostics_count",
        unadmitted_compute_rows_with_diagnostics_count(),
    );
    push_count_field(
        fields,
        "unadmitted_compute_rows_missing_diagnostics_count",
        unadmitted_compute_rows_missing_diagnostics_count(),
    );
    push_field(
        fields,
        "native_unsupported_coverage_policy_refs",
        "docs/architecture/vortex-upstream-alignment-hardening.md,docs/architecture/operational-evidence-policy-hardening.md",
    );
    push_field(
        fields,
        "native_unsupported_coverage_benchmark_refs",
        "benchmarks/traditional_analytics coverage_table unsupported_diagnostic_code fields",
    );
}

fn append_predicate_dtype_coverage_fields(fields: &mut Vec<(String, String)>) {
    append_predicate_dtype_coverage_summary_fields(fields);
    append_predicate_dtype_coverage_invariant_fields(fields);
    for row in PREDICATE_DTYPE_COVERAGE_ROWS {
        append_predicate_dtype_coverage_row_fields(fields, row);
    }
}

fn append_predicate_dtype_coverage_summary_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "predicate_dtype_coverage_schema_version",
        PREDICATE_DTYPE_COVERAGE_SCHEMA_VERSION,
    );
    push_field(
        fields,
        "predicate_dtype_coverage_status",
        "complete_for_current_matrix",
    );
    push_field(
        fields,
        "predicate_dtype_coverage_scope",
        "predicate_dtype_null_nested_statistics_current_runtime_readiness",
    );
    push_field(
        fields,
        "predicate_dtype_coverage_support_status_vocabulary",
        PREDICATE_DTYPE_COVERAGE_SUPPORT_STATUS_VOCABULARY,
    );
    push_field(
        fields,
        "predicate_dtype_coverage_category_vocabulary",
        PREDICATE_DTYPE_COVERAGE_CATEGORY_VOCABULARY,
    );
    push_count_field(
        fields,
        "predicate_dtype_coverage_row_count",
        PREDICATE_DTYPE_COVERAGE_ROWS.len(),
    );
    push_field(
        fields,
        "predicate_dtype_coverage_row_order",
        &predicate_dtype_coverage_row_order(),
    );
    for category in [
        "predicate",
        "dtype",
        "null_semantics",
        "nested_shape",
        "statistics",
    ] {
        push_count_field(
            fields,
            &format!("predicate_dtype_coverage_{category}_count"),
            predicate_dtype_category_count(category),
        );
    }
    for status in [
        "fixture_certified",
        "executable_uncertified",
        "fixture_needed",
        "unsupported",
    ] {
        push_count_field(
            fields,
            &format!("predicate_dtype_coverage_{status}_count"),
            predicate_dtype_support_status_count(status),
        );
    }
}

fn append_predicate_dtype_coverage_invariant_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "predicate_dtype_coverage_claim_gate_status",
        "not_claim_grade",
    );
    push_bool_field(
        fields,
        "predicate_dtype_coverage_current_matrix_complete",
        true,
    );
    push_bool_field(
        fields,
        "predicate_dtype_coverage_all_rows_have_support_status",
        true,
    );
    push_bool_field(
        fields,
        "predicate_dtype_coverage_all_rows_have_evidence_gap",
        true,
    );
    push_bool_field(
        fields,
        "predicate_dtype_coverage_all_rows_fallback_attempted_false",
        true,
    );
    push_bool_field(
        fields,
        "predicate_dtype_coverage_all_rows_external_engine_invoked_false",
        true,
    );
    push_bool_field(fields, "predicate_dtype_coverage_runtime_execution", false);
    push_bool_field(fields, "predicate_dtype_coverage_data_read", false);
    push_bool_field(fields, "predicate_dtype_coverage_data_materialized", false);
    push_bool_field(fields, "predicate_dtype_coverage_write_io", false);
    push_bool_field(fields, "predicate_dtype_coverage_fallback_attempted", false);
    push_bool_field(
        fields,
        "predicate_dtype_coverage_external_engine_invoked",
        false,
    );
    push_field(
        fields,
        "predicate_dtype_coverage_benchmark_refs",
        "docs/architecture/benchmark-suite-catalog.md,benchmarks/traditional_analytics coverage_table",
    );
    push_field(
        fields,
        "predicate_dtype_coverage_correctness_refs",
        "docs/rfcs/0006-statistics-pruning-metadata-only-execution.md,query_primitive_correctness,correctness_fixture_manifest",
    );
    push_field(
        fields,
        "predicate_dtype_coverage_next_runtime_slice",
        "select one fixture_needed row and promote it with correctness, benchmark, certificate, Native I/O, and no-fallback evidence",
    );
}

fn append_predicate_dtype_coverage_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &PredicateDtypeCoverageRow,
) {
    let prefix = format!("predicate_dtype_coverage_row_{}", row.id);
    push_field(fields, &format!("{prefix}_category"), row.category);
    push_field(fields, &format!("{prefix}_family"), row.family);
    push_field(fields, &format!("{prefix}_surface"), row.surface);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_runtime_surface"),
        row.runtime_surface,
    );
    push_field(
        fields,
        &format!("{prefix}_statistics_required"),
        row.statistics_required,
    );
    push_field(
        fields,
        &format!("{prefix}_fixture_status"),
        row.fixture_status,
    );
    push_field(
        fields,
        &format!("{prefix}_correctness_refs"),
        row.correctness_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_benchmark_refs"),
        row.benchmark_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_execution_certificate_refs"),
        row.execution_certificate_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_native_io_refs"),
        row.native_io_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_materialization_decode_refs"),
        row.materialization_decode_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code"),
        row.unsupported_diagnostic_code,
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
    push_bool_field(fields, &format!("{prefix}_execution_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_fallback_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_external_engine_invoked"), false);
}

fn append_materialization_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &MaterializationPolicyReport,
) {
    push_field(
        fields,
        "materialization_policy_schema_version",
        report.schema_version,
    );
    push_field(fields, "materialization_policy_report_id", report.report_id);
    push_field(
        fields,
        "materialization_policy_report_ref",
        report.report_ref,
    );
    push_field(fields, "materialization_policy_docs_ref", report.docs_ref);
    push_field(
        fields,
        "materialization_policy_support_status_vocabulary",
        report.support_status_vocabulary,
    );
    push_field(
        fields,
        "materialization_policy_operator_execution_class_vocabulary",
        report.operator_execution_class_vocabulary,
    );
    push_field(
        fields,
        "materialization_policy_claim_gate_status",
        report.claim_gate_status,
    );
    push_count_field(
        fields,
        "materialization_policy_row_count",
        report.rows.len(),
    );
    push_field(
        fields,
        "materialization_policy_row_order",
        &report.row_order().join(","),
    );
    push_field(
        fields,
        "materialization_policy_operator_execution_classes",
        &report.operator_execution_classes().join(","),
    );
    push_field(
        fields,
        "materialization_policy_blocker_ids",
        &report.blocker_ids().join(","),
    );
    push_bool_field(
        fields,
        "materialization_policy_all_rows_classified",
        report.all_rows_classified(),
    );
    push_bool_field(
        fields,
        "materialization_policy_all_rows_fallback_attempted_false",
        report.all_rows_fallback_free(),
    );
    push_bool_field(
        fields,
        "materialization_policy_all_rows_external_engine_invoked_false",
        report.all_rows_fallback_free(),
    );
    push_bool_field(
        fields,
        "materialization_policy_runtime_execution",
        report.runtime_execution,
    );
    push_bool_field(
        fields,
        "materialization_policy_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "materialization_policy_external_engine_invoked",
        report.external_engine_invoked,
    );
    for row in &report.rows {
        append_materialization_policy_row_fields(fields, row);
    }
}

fn append_materialization_policy_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &shardloom_core::MaterializationPolicyRow,
) {
    let prefix = format!("materialization_policy_row_{}", row.row_id);
    push_field(
        fields,
        &format!("{prefix}_operator_execution_class"),
        row.operator_execution_class.as_str(),
    );
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_bool_field(fields, &format!("{prefix}_data_decoded"), row.data_decoded);
    push_bool_field(
        fields,
        &format!("{prefix}_data_materialized"),
        row.data_materialized,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_stayed_encoded"),
        row.stayed_encoded,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_boundary_required"),
        row.materialization_boundary_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_boundary_emitted"),
        row.materialization_boundary_emitted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_materialized_temporary_path"),
        row.materialized_temporary_path,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_encoded_native_claim_allowed"),
        row.encoded_native_claim_allowed,
    );
    push_field(
        fields,
        &format!("{prefix}_materialization_decode_refs"),
        row.materialization_decode_refs,
    );
    push_field(fields, &format!("{prefix}_policy_refs"), row.policy_refs);
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code"),
        row.unsupported_diagnostic_code,
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_execution"),
        row.runtime_execution,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        row.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        row.external_engine_invoked,
    );
}

fn append_compute_capability_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &ComputeCapabilityRow,
) {
    let prefix = format!("compute_row_{}", row.id);
    push_field(fields, &format!("{prefix}_surface"), row.surface);
    push_field(fields, &format!("{prefix}_family"), row.family);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(fields, &format!("{prefix}_engine_mode"), row.engine_mode);
    push_field(
        fields,
        &format!("{prefix}_execution_mode"),
        compute_row_execution_mode(row),
    );
    push_field(
        fields,
        &format!("{prefix}_provider_kind"),
        row.provider_kind,
    );
    push_field(
        fields,
        &format!("{prefix}_semantic_profile"),
        row.semantic_profile,
    );
    push_field(
        fields,
        &format!("{prefix}_materialization_decode_requirement"),
        row.materialization_decode_requirement,
    );
    push_field(
        fields,
        &format!("{prefix}_memory_spill_requirement"),
        row.memory_spill_requirement,
    );
    push_field(
        fields,
        &format!("{prefix}_correctness_refs"),
        row.correctness_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_benchmark_refs"),
        row.benchmark_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_execution_certificate_refs"),
        row.execution_certificate_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_native_io_refs"),
        row.native_io_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code"),
        row.unsupported_diagnostic_code,
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        compute_row_claim_gate_status(row),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_vortex_native_claim_allowed"),
        compute_row_vortex_native_claim_allowed(row),
    );
    push_field(
        fields,
        &format!("{prefix}_operator_execution_class"),
        compute_row_operator_execution_class(row),
    );
    push_field(
        fields,
        &format!("{prefix}_operator_blocker_id"),
        compute_row_operator_blocker_id(row),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_operator_encoded_native_claim_allowed"),
        compute_row_operator_encoded_native_claim_allowed(row),
    );
    push_bool_field(fields, &format!("{prefix}_fallback_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_external_engine_invoked"), false);
}

fn compute_row_execution_mode(row: &ComputeCapabilityRow) -> &'static str {
    match row.id {
        "compatibility_csv_import" | "vortex_sink_write" => "compatibility_import_certified",
        "prepared_encoded_filter"
        | "reader_backed_dictionary_filter"
        | "grouped_aggregate"
        | "join"
        | "window_row_number" => "prepared_vortex",
        "direct_compatibility_transient" => "direct_compatibility_transient",
        "sql_frontend" => "auto",
        _ => "native_vortex",
    }
}

fn compute_row_claim_gate_status(row: &ComputeCapabilityRow) -> &'static str {
    match row.support_status {
        "fixture_certified" => "fixture_smoke_only",
        "workload_certified" | "production_certified" => "claim_grade",
        _ => "not_claim_grade",
    }
}

fn compute_row_vortex_native_claim_allowed(row: &ComputeCapabilityRow) -> bool {
    matches!(
        compute_row_execution_mode(row),
        "prepared_vortex" | "native_vortex"
    ) && !matches!(
        row.support_status,
        "unsupported" | "planned" | "report_only"
    )
}

fn compute_row_operator_execution_class(row: &ComputeCapabilityRow) -> &'static str {
    match row.support_status {
        "unsupported" | "planned" | "report_only" => "unsupported",
        _ if row
            .materialization_decode_requirement
            .contains("no_row_materialization") =>
        {
            "encoded_native"
        }
        _ if row
            .materialization_decode_requirement
            .contains("materialization") =>
        {
            "materialized_temporary"
        }
        _ => "residual_native",
    }
}

fn compute_row_operator_blocker_id(row: &ComputeCapabilityRow) -> &'static str {
    if compute_row_operator_execution_class(row) == "encoded_native" {
        "none"
    } else {
        row.blocker_id
    }
}

fn compute_row_operator_encoded_native_claim_allowed(row: &ComputeCapabilityRow) -> bool {
    compute_row_operator_execution_class(row) == "encoded_native"
        && matches!(
            row.support_status,
            "fixture_certified" | "workload_certified" | "production_certified"
        )
}

fn append_operator_family_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &OperatorFamilyCoverageRow,
) {
    let prefix = format!("operator_family_{}", row.id);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_next_evidence"),
        row.next_evidence,
    );
}

fn append_native_unsupported_coverage_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &NativeUnsupportedCoverageRow,
) {
    let prefix = format!("native_unsupported_coverage_row_{}", row.id);
    push_field(fields, &format!("{prefix}_category"), row.category);
    push_field(fields, &format!("{prefix}_surface"), row.surface);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code"),
        row.unsupported_diagnostic_code,
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    );
    push_field(fields, &format!("{prefix}_source_refs"), row.source_refs);
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        "not_claim_grade",
    );
    push_bool_field(fields, &format!("{prefix}_execution_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_fallback_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_external_engine_invoked"), false);
}

fn append_native_vortex_admission_lane_fields(
    fields: &mut Vec<(String, String)>,
    lane: &NativeVortexAdmissionLane,
) {
    append_native_vortex_admission_lane_identity_fields(fields, lane);
    append_native_vortex_admission_lane_evidence_fields(fields, lane);
    append_native_vortex_admission_lane_claim_fields(fields, lane);
}

fn append_native_vortex_admission_lane_identity_fields(
    fields: &mut Vec<(String, String)>,
    lane: &NativeVortexAdmissionLane,
) {
    let prefix = format!("native_vortex_admission_lane_{}", lane.id);
    push_field(
        fields,
        &format!("{prefix}_source_surface"),
        lane.source_surface,
    );
    push_field(
        fields,
        &format!("{prefix}_operator_surface"),
        lane.operator_surface,
    );
    push_field(fields, &format!("{prefix}_sink_surface"), lane.sink_surface);
    push_field(
        fields,
        &format!("{prefix}_admission_status"),
        lane.admission_status,
    );
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        lane.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_execution_mode"),
        lane.execution_mode,
    );
    push_field(
        fields,
        &format!("{prefix}_provider_kind"),
        lane.provider_kind,
    );
    push_field(
        fields,
        &format!("{prefix}_provider_api_surface"),
        lane.provider_api_surface,
    );
    push_field(
        fields,
        &format!("{prefix}_provider_crate"),
        lane.provider_crate,
    );
    push_field(
        fields,
        &format!("{prefix}_provider_version"),
        lane.provider_version,
    );
    push_field(fields, &format!("{prefix}_feature_gate"), lane.feature_gate);
    push_field(
        fields,
        &format!("{prefix}_shardloom_admission_policy"),
        lane.shardloom_admission_policy,
    );
}

fn append_native_vortex_admission_lane_evidence_fields(
    fields: &mut Vec<(String, String)>,
    lane: &NativeVortexAdmissionLane,
) {
    let prefix = format!("native_vortex_admission_lane_{}", lane.id);
    push_field(
        fields,
        &format!("{prefix}_compute_row_ref"),
        lane.compute_row_ref,
    );
    push_field(
        fields,
        &format!("{prefix}_benchmark_ref"),
        lane.benchmark_ref,
    );
    push_field(
        fields,
        &format!("{prefix}_correctness_refs"),
        lane.correctness_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_execution_certificate_refs"),
        lane.execution_certificate_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_native_io_refs"),
        lane.native_io_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_materialization_decode_refs"),
        lane.materialization_decode_refs,
    );
    push_field(fields, &format!("{prefix}_policy_refs"), lane.policy_refs);
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        lane.required_future_evidence,
    );
}

fn append_native_vortex_admission_lane_claim_fields(
    fields: &mut Vec<(String, String)>,
    lane: &NativeVortexAdmissionLane,
) {
    let prefix = format!("native_vortex_admission_lane_{}", lane.id);
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        lane.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        lane.claim_boundary,
    );
    push_field(
        fields,
        &format!("{prefix}_residual_executor"),
        lane.residual_executor,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_vortex_native_claim_allowed"),
        true,
    );
    push_bool_field(fields, &format!("{prefix}_fallback_attempted"), false);
    push_bool_field(fields, &format!("{prefix}_external_engine_invoked"), false);
    push_bool_field(fields, &format!("{prefix}_object_store_io"), false);
    push_bool_field(fields, &format!("{prefix}_write_io"), false);
}

fn compute_support_status_count(status: &str) -> usize {
    COMPUTE_ROWS
        .iter()
        .filter(|row| row.support_status == status)
        .count()
}

fn native_unsupported_category_count(category: &str) -> usize {
    NATIVE_UNSUPPORTED_COVERAGE_ROWS
        .iter()
        .filter(|row| row.category == category)
        .count()
}

fn predicate_dtype_category_count(category: &str) -> usize {
    PREDICATE_DTYPE_COVERAGE_ROWS
        .iter()
        .filter(|row| row.category == category)
        .count()
}

fn predicate_dtype_support_status_count(status: &str) -> usize {
    PREDICATE_DTYPE_COVERAGE_ROWS
        .iter()
        .filter(|row| row.support_status == status)
        .count()
}

fn unadmitted_compute_row_count() -> usize {
    COMPUTE_ROWS
        .iter()
        .filter(|row| compute_row_requires_unsupported_diagnostic(row))
        .count()
}

fn unadmitted_compute_rows_with_diagnostics_count() -> usize {
    COMPUTE_ROWS
        .iter()
        .filter(|row| {
            compute_row_requires_unsupported_diagnostic(row)
                && row.unsupported_diagnostic_code != "none"
                && row.blocker_id != "none"
                && row.required_future_evidence != "none"
        })
        .count()
}

fn unadmitted_compute_rows_missing_diagnostics_count() -> usize {
    unadmitted_compute_row_count().saturating_sub(unadmitted_compute_rows_with_diagnostics_count())
}

fn compute_row_requires_unsupported_diagnostic(row: &ComputeCapabilityRow) -> bool {
    matches!(
        row.support_status,
        "unsupported" | "planned" | "report_only"
    )
}

fn compute_row_order() -> String {
    COMPUTE_ROWS
        .iter()
        .map(|row| row.id)
        .collect::<Vec<_>>()
        .join(",")
}

fn native_vortex_admission_lane_order() -> String {
    NATIVE_VORTEX_ADMISSION_LANES
        .iter()
        .map(|lane| lane.id)
        .collect::<Vec<_>>()
        .join(",")
}

fn operator_family_order() -> String {
    OPERATOR_FAMILY_ROWS
        .iter()
        .map(|row| row.id)
        .collect::<Vec<_>>()
        .join(",")
}

fn native_unsupported_row_order() -> String {
    NATIVE_UNSUPPORTED_COVERAGE_ROWS
        .iter()
        .map(|row| row.id)
        .collect::<Vec<_>>()
        .join(",")
}

fn predicate_dtype_coverage_row_order() -> String {
    PREDICATE_DTYPE_COVERAGE_ROWS
        .iter()
        .map(|row| row.id)
        .collect::<Vec<_>>()
        .join(",")
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
    append_sql_dataframe_planner_readiness_fields(fields);
}

fn append_sql_dataframe_planner_readiness_fields(fields: &mut Vec<(String, String)>) {
    let matrix = SqlDataFramePlannerReadinessMatrix::report_only();
    push_field(
        fields,
        "planner_readiness_schema_version",
        matrix.schema_version,
    );
    push_field(fields, "planner_readiness_matrix_id", matrix.matrix_id);
    push_field(fields, "planner_readiness_report_ref", matrix.report_ref);
    push_field(fields, "planner_readiness_docs_ref", matrix.docs_ref);
    push_field(
        fields,
        "planner_readiness_support_status_vocabulary",
        matrix.support_status_vocabulary,
    );
    push_field(
        fields,
        "planner_readiness_claim_gate_status",
        matrix.claim_gate_status,
    );
    push_count_field(fields, "planner_readiness_row_count", matrix.rows.len());
    push_field(
        fields,
        "planner_readiness_row_order",
        &matrix.row_order().join(","),
    );
    push_field(
        fields,
        "planner_readiness_sql_row_order",
        &matrix.sql_row_order().join(","),
    );
    push_field(
        fields,
        "planner_readiness_dataframe_row_order",
        &matrix.dataframe_row_order().join(","),
    );
    push_field(
        fields,
        "planner_readiness_unsupported_diagnostic_codes",
        &matrix.unsupported_diagnostic_codes().join(","),
    );
    push_field(
        fields,
        "planner_readiness_blocker_ids",
        &matrix.blocker_ids().join(","),
    );
    push_field(
        fields,
        "planner_readiness_required_evidence",
        &matrix.required_evidence().join("|"),
    );
    push_bool_field(fields, "planner_readiness_parser_executed", false);
    push_bool_field(fields, "planner_readiness_binder_executed", false);
    push_bool_field(fields, "planner_readiness_planner_executed", false);
    push_bool_field(fields, "planner_readiness_runtime_execution", false);
    push_bool_field(fields, "planner_readiness_dataframe_runtime", false);
    push_bool_field(fields, "planner_readiness_external_engine_invoked", false);
    push_bool_field(fields, "planner_readiness_fallback_attempted", false);
    push_bool_field(
        fields,
        "planner_readiness_deterministic_diagnostics_present",
        matrix.deterministic_diagnostics_present(),
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

#[allow(clippy::too_many_lines)]
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
    let mut fields = vec![
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
    ];
    if scope == CapabilityDiscoveryScope::DataFrame {
        append_sql_dataframe_planner_readiness_fields(&mut fields);
    }
    fields
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
