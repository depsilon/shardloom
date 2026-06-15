use std::process::Command;

fn run_cg10_object_store_runtime_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg10-object-store-runtime-gate", "--format", "json"])
        .output()
        .expect("cg10-object-store-runtime-gate command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn assert_false_fields(output: &str, keys: &[&str]) {
    for key in keys {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
}

fn assert_true_fields(output: &str, keys: &[&str]) {
    for key in keys {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
}

fn assert_runtime_gate_false_fields(output: &str) {
    assert_false_fields(
        output,
        &[
            "range_read_execution_allowed",
            "full_file_read_allowed",
            "request_coalescing_runtime_allowed",
            "coordinator_start_allowed",
            "worker_start_allowed",
            "task_execution_allowed",
            "retry_execution_allowed",
            "checkpoint_write_allowed",
            "cleanup_execution_allowed",
            "commit_execution_allowed",
            "credential_resolution_allowed",
            "object_store_io_allowed",
            "data_read_allowed",
            "write_io_allowed",
            "approved_real_backend_profile_declared",
            "approved_real_backend_network_access_allowed",
            "approved_real_backend_credential_resolution_allowed",
            "approved_real_backend_read_allowed",
            "approved_real_backend_write_allowed",
            "production_object_store_native_io_certificate_present",
            "production_object_store_claim_allowed",
            "object_store_runtime_claim_allowed",
            "distributed_runtime_claim_allowed",
            "fallback_attempted",
            "fallback_execution_allowed",
        ],
    );
}

fn assert_runtime_gate_true_fields(output: &str) {
    assert_true_fields(
        output,
        &[
            "range_planning_evidence_required",
            "request_budget_policy_required",
            "provider_capability_policy_required",
            "credential_effect_policy_required",
            "scheduler_policy_required",
            "worker_identity_required",
            "checkpoint_plan_required",
            "retry_policy_required",
            "idempotency_keys_required",
            "attempt_records_required",
            "cleanup_policy_required",
            "atomic_commit_evidence_required",
            "execution_certificate_required",
            "native_io_certificate_required",
            "benchmark_evidence_required",
            "approved_real_backend_profile_required",
            "runtime_promotions_blocked",
            "claim_blocked",
            "side_effect_free",
            "plan_only",
        ],
    );
}

fn assert_byte_range_provider_gate_fields(output: &str) {
    assert_false_fields(
        output,
        &[
            "byte_range_provider_gate_range_read_execution_allowed",
            "byte_range_provider_gate_full_file_read_allowed",
            "byte_range_provider_gate_credential_resolution_allowed",
            "byte_range_provider_gate_credentials_resolved",
            "byte_range_provider_gate_retry_execution_allowed",
            "byte_range_provider_gate_provider_probe",
            "byte_range_provider_gate_network_probe",
            "byte_range_provider_gate_data_read",
            "byte_range_provider_gate_object_store_io",
            "byte_range_provider_gate_write_io",
            "byte_range_provider_gate_fallback_attempted",
            "byte_range_provider_gate_fallback_execution_allowed",
            "byte_range_provider_gate_external_engine_invoked",
        ],
    );
    assert_true_fields(
        output,
        &[
            "byte_range_provider_gate_range_planning_evidence_present",
            "byte_range_provider_gate_request_budget_policy_required",
            "byte_range_provider_gate_provider_capability_policy_required",
            "byte_range_provider_gate_credential_policy_required",
            "byte_range_provider_gate_retry_policy_required",
            "byte_range_provider_gate_idempotency_key_required",
            "byte_range_provider_gate_execution_certificate_required",
            "byte_range_provider_gate_native_io_certificate_required",
            "byte_range_provider_gate_benchmark_evidence_required",
            "byte_range_provider_gate_side_effect_free",
        ],
    );
    assert!(output.contains(&field(
        "byte_range_provider_gate_required_evidence",
        "provider_capability_policy,credential_effect_policy,request_budget_policy,retry_policy,idempotency_key_contract,execution_certificate,native_io_certificate,benchmark_evidence"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_claim_gate_status",
        "not_claim_grade"
    )));
}

fn assert_runtime_blocker_matrix_fields(output: &str) {
    assert_false_fields(
        output,
        &[
            "runtime_blocker_matrix_row_coordinator_start_allowed",
            "runtime_blocker_matrix_row_worker_start_allowed",
            "runtime_blocker_matrix_row_task_execution_allowed",
            "runtime_blocker_matrix_row_checkpoint_write_allowed",
            "runtime_blocker_matrix_row_retry_attempt_allowed",
            "runtime_blocker_matrix_row_cleanup_execution_allowed",
            "runtime_blocker_matrix_row_commit_record_write_allowed",
            "runtime_blocker_matrix_row_partition_discovery_allowed",
            "runtime_blocker_matrix_row_catalog_integration_allowed",
            "runtime_blocker_matrix_row_remote_result_delivery_allowed",
            "runtime_blocker_matrix_row_coordinator_start_coordinator_started",
            "runtime_blocker_matrix_row_worker_start_worker_started",
            "runtime_blocker_matrix_row_task_execution_task_executed",
            "runtime_blocker_matrix_row_checkpoint_write_checkpoint_written",
            "runtime_blocker_matrix_row_retry_attempt_retry_attempted",
            "runtime_blocker_matrix_row_cleanup_execution_cleanup_executed",
            "runtime_blocker_matrix_row_commit_record_write_commit_record_written",
            "runtime_blocker_matrix_row_partition_discovery_object_store_io",
            "runtime_blocker_matrix_row_catalog_integration_external_engine_invoked",
            "runtime_blocker_matrix_row_remote_result_delivery_write_io",
            "runtime_blocker_matrix_row_retry_attempt_fallback_attempted",
            "runtime_blocker_matrix_row_commit_record_write_external_engine_invoked",
        ],
    );
    assert_true_fields(
        output,
        &[
            "runtime_blocker_matrix_all_allowed_false",
            "runtime_blocker_matrix_all_no_io",
            "runtime_blocker_matrix_all_no_fallback",
            "runtime_blocker_matrix_all_no_external_engine",
            "runtime_blocker_matrix_row_coordinator_start_side_effect_free",
            "runtime_blocker_matrix_row_worker_start_side_effect_free",
            "runtime_blocker_matrix_row_task_execution_side_effect_free",
            "runtime_blocker_matrix_row_checkpoint_write_side_effect_free",
            "runtime_blocker_matrix_row_retry_attempt_side_effect_free",
            "runtime_blocker_matrix_row_cleanup_execution_side_effect_free",
            "runtime_blocker_matrix_row_commit_record_write_side_effect_free",
            "runtime_blocker_matrix_row_partition_discovery_side_effect_free",
            "runtime_blocker_matrix_row_catalog_integration_side_effect_free",
            "runtime_blocker_matrix_row_remote_result_delivery_side_effect_free",
        ],
    );
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_retry_attempt_diagnostic_code",
        "SL_OBJECT_STORE_UNSUPPORTED"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_retry_attempt_blocker_id",
        "gar0008b.retry_attempt_blocked"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_retry_attempt_required_evidence",
        "retry_policy,retryable_failure_classes,attempt_records,idempotency_key_contract"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_commit_record_write_required_evidence",
        "commit_record_schema,atomic_commit_evidence,cleanup_policy,idempotency_key_contract"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_commit_record_write_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_partition_discovery_required_evidence",
        "partition_listing_policy,partition_schema_contract,credential_effect_policy,execution_certificate,native_io_certificate,no_fallback_policy"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_catalog_integration_blocker_id",
        "gar0008b.catalog_integration_blocked"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_catalog_integration_required_evidence",
        "catalog_adapter_policy,catalog_auth_policy,snapshot_consistency_contract,execution_certificate,native_io_certificate,no_fallback_policy"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_remote_result_delivery_required_evidence",
        "remote_delivery_protocol,result_replay_policy,idempotency_key_contract,credential_effect_policy,execution_certificate,native_io_certificate,no_fallback_policy"
    )));
}

#[test]
#[allow(clippy::too_many_lines)]
fn cg10_object_store_runtime_gate_exposes_surface_order_and_existing_evidence() {
    let output = run_cg10_object_store_runtime_gate_json();

    assert!(output.contains("\"command\":\"cg10-object-store-runtime-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "object_store_runtime_promotion_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_runtime_promotion_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg10.object_store_runtime_promotion_gate"
    )));
    assert!(output.contains(&field("surface_count", "16")));
    assert!(output.contains(&field("existing_evidence_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "14")));
    assert!(output.contains(&field(
        "surface_order",
        "request_planner_aggregate,byte_range_provider_gate,range_read_execution,request_coalescing_runtime,distributed_coordinator_startup,distributed_worker_startup,distributed_task_execution,checkpoint_write_execution,retry_execution,cleanup_execution,object_store_commit_execution,partition_discovery_runtime,catalog_integration_runtime,remote_result_delivery_runtime,provider_credential_runtime,benchmark_certificate_closeout"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_report_id",
        "gar0008a.object_store_byte_range_provider_gate"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_status",
        "blocked_until_certified"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_schema_version",
        "shardloom.object_store_runtime_blocker_matrix.v1"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_report_id",
        "gar0008b.object_store_runtime_blocker_matrix"
    )));
    assert!(output.contains(&field("runtime_blocker_matrix_row_count", "10")));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_row_order",
        "coordinator_start,worker_start,task_execution,checkpoint_write,retry_attempt,cleanup_execution,commit_record_write,partition_discovery,catalog_integration,remote_result_delivery"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_diagnostics_propagated",
        "true"
    )));
    assert!(output.contains(&field("runtime_blocker_matrix_diagnostic_count", "10")));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_diagnostic_code_order",
        "SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED,SL_OBJECT_STORE_UNSUPPORTED"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_diagnostic_category_order",
        "object_store,object_store,object_store,object_store,object_store,object_store,object_store,object_store,object_store,object_store"
    )));
    assert!(output.contains(&field(
        "runtime_blocker_matrix_diagnostic_severity_order",
        "info,info,info,info,info,info,info,info,info,info"
    )));
    assert!(output.contains(&field("runtime_blocker_matrix_envelope_status", "success")));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains("\"severity\":\"info\""));
    assert!(output.contains("\"category\":\"object_store\""));
    assert!(output.contains("\"feature\":\"coordinator_start\""));
    assert!(output.contains("\"feature\":\"commit_record_write\""));
    assert!(output.contains("\"feature\":\"partition_discovery\""));
    assert!(output.contains("\"feature\":\"catalog_integration\""));
    assert!(output.contains("\"feature\":\"remote_result_delivery\""));
    assert!(output.contains(&field("existing_request_planner_evidence_present", "true")));
    assert!(output.contains(&field("existing_range_planning_evidence_present", "true")));
    assert!(output.contains(&field("existing_coalescing_evidence_present", "true")));
    assert!(output.contains(&field(
        "existing_distributed_scheduling_evidence_present",
        "true"
    )));
    assert!(output.contains(&field("existing_checkpoint_retry_evidence_present", "true")));
    assert!(output.contains(&field("existing_commit_protocol_evidence_present", "true")));
    assert!(output.contains(&field(
        "existing_local_emulator_partition_discovery_evidence_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_partition_discovery_command",
        "object-store-partition-discovery-smoke"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_partition_discovery_certificate_id",
        "gar-runtime-impl-6d.local_emulator_partition_discovery.native_io"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_partition_discovery_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "local_emulator_partition_discovery_runtime_supported",
        "true"
    )));
    assert!(output.contains(&field(
        "live_provider_partition_discovery_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_write_recovery_evidence_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_write_recovery_command",
        "object-store-write-recovery-smoke"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_write_recovery_certificate_id",
        "gar-runtime-impl-6d.local_emulator_object_store_write_recovery.native_io"
    )));
    assert!(output.contains(&field(
        "existing_local_emulator_write_recovery_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "local_emulator_write_recovery_runtime_supported",
        "true"
    )));
    assert!(output.contains(&field(
        "live_provider_write_recovery_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field("approved_real_backend_profile_id", "not_declared")));
    assert!(output.contains(&field(
        "approved_real_backend_profile_status",
        "missing_approved_real_backend_profile"
    )));
    assert!(output.contains(&field(
        "approved_real_backend_required_evidence",
        "approved_backend_id,credential_policy,redaction_policy,network_probe_policy,byte_range_read_certificate,write_commit_recovery_certificate,retry_backoff_policy,rate_limit_policy,bounded_streaming_evidence,benchmark_profile"
    )));
    assert!(output.contains(&field(
        "production_object_store_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "production_object_store_blocker_id",
        "prod-ready-1b.approved_real_backend_profile_missing"
    )));
}

#[test]
fn cg10_object_store_runtime_gate_blocks_execution_io_credentials_and_claims() {
    let output = run_cg10_object_store_runtime_gate_json();

    assert_runtime_gate_false_fields(&output);
    assert_runtime_gate_true_fields(&output);
    assert_byte_range_provider_gate_fields(&output);
    assert_runtime_blocker_matrix_fields(&output);
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("diagnostic_count", "10")));
}
