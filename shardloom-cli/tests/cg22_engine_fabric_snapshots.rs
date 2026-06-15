use std::process::Command;

fn run_json(args: &[&str], success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .args(["--format", "json"])
        .output()
        .expect("shardloom command runs");
    assert_eq!(
        output.status.success(),
        success,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn engine_selection_auto_snapshot_selects_batch_without_runtime_or_fallback() {
    let output = run_json(&["engine-selection-plan"], true);

    assert!(output.contains("\"command\":\"engine-selection-plan\""));
    assert!(output.contains(&field("requested_engine_mode", "auto")));
    assert!(output.contains(&field("selection_status", "selected")));
    assert!(output.contains(&field("selected_engine_mode", "batch")));
    assert!(output.contains(&field("allowed_engine_modes", "batch")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains("\"diagnostics\":[]"));
}

#[test]
fn engine_selection_live_update_selects_live_fixture_without_fallback() {
    let output = run_json(
        &[
            "engine-selection-plan",
            "live",
            "unbounded",
            "append-only",
            "update",
        ],
        true,
    );

    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("requested_engine_mode", "live")));
    assert!(output.contains(&field("selection_status", "selected")));
    assert!(output.contains(&field("selected_engine_mode", "live")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"diagnostics\":[]"));
}

#[test]
fn engine_selection_live_changelog_rejects_unemitted_output_mode_without_fallback() {
    let output = run_json(
        &[
            "engine-selection-plan",
            "live",
            "unbounded",
            "append-only",
            "changelog",
        ],
        false,
    );

    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("requested_engine_mode", "live")));
    assert!(output.contains(&field("selection_status", "rejected")));
    assert!(output.contains(&field("selected_engine_mode", "none")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("live fixture requires update or continuous-view output modes"));
}

#[test]
fn engine_capability_matrix_separates_batch_live_and_hybrid_claims() {
    let output = run_json(&["engine-capability-matrix"], true);

    assert!(output.contains("\"command\":\"engine-capability-matrix\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.engine_capability_matrix.v1"
    )));
    assert!(output.contains(&field("engine_modes", "batch,live,hybrid")));
    assert!(output.contains(&field("batch_support_status", "partially_supported")));
    assert!(output.contains(&field("live_support_status", "partially_supported")));
    assert!(output.contains(&field("hybrid_support_status", "partially_supported")));
    assert!(output.contains(&field("partially_supported_engine_count", "3")));
    assert!(output.contains(&field("planned_engine_count", "0")));
    assert!(output.contains(&field("live_hybrid_claim_blocked_count", "2")));
    assert!(output.contains(&field("live_state_required", "true")));
    assert!(output.contains(&field("hybrid_changelog_support", "true")));
    assert!(output.contains(&field("hybrid_checkpoint_required", "true")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_report_id",
        "gar0013.streaming_runtime_capability_matrix"
    )));
    assert!(output.contains(&field("streaming_capability_matrix_row_count", "8")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_diagnostic_code_order",
        "SL_OBJECT_STORE_UNSUPPORTED,SL_MATERIALIZATION_REQUIRED,SL_NOT_IMPLEMENTED"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
        "true"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_schema_version",
        "shardloom.live_hybrid_fabric_freshness_gate.v1"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_report_id",
        "gar-0034-a.live_hybrid_fabric_freshness_gate"
    )));
    assert!(output.contains(&field("live_hybrid_fabric_gate_row_count", "10")));
    assert!(output.contains(&field("live_hybrid_fabric_gate_blocked_row_count", "7")));
    assert!(output.contains(&field("live_hybrid_fabric_gate_report_only_row_count", "1")));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_fixture_smoke_row_count",
        "2"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_freshness_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_exactly_once_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_object_store_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_broker_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_state_store_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_baseline_oracle_only",
        "true"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "live_hybrid_fabric_gate_external_engine_invoked",
        "false"
    )));
}

#[test]
fn engine_selection_hybrid_overlay_selects_hybrid_fixture_without_fallback() {
    let output = run_json(
        &[
            "engine-selection-plan",
            "hybrid",
            "snapshot",
            "upsert",
            "continuous-view",
        ],
        true,
    );

    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("requested_engine_mode", "hybrid")));
    assert!(output.contains(&field("selection_status", "selected")));
    assert!(output.contains(&field("selected_engine_mode", "hybrid")));
    assert!(output.contains(&field("allowed_engine_modes", "hybrid")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"diagnostics\":[]"));
}

#[test]
fn live_change_contract_plan_declares_change_and_policy_vocabulary() {
    let output = run_json(&["live-change-contract-plan"], true);

    assert!(output.contains("\"command\":\"live-change-contract-plan\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.live_change_contract.v1"
    )));
    assert!(output.contains(&field("mode", "live_change_contract_plan")));
    assert!(output.contains(&field(
        "change_record_field_order",
        "key,operation,sequence,event_time_ms,processing_time_ms,source_offset,schema_digest,payload_ref"
    )));
    assert!(output.contains(&field(
        "change_operation_vocabulary",
        "append,upsert,delete,retract,tombstone"
    )));
    assert!(output.contains(&field("watermark_policy", "fixture_event_time")));
    assert!(output.contains(&field(
        "checkpoint_policy",
        "in_memory_deterministic_fixture"
    )));
    assert!(output.contains(&field(
        "fixture_operator_vocabulary",
        "filter,project,count,count_where,group_count"
    )));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn live_fixture_run_group_count_emits_state_freshness_and_certificate_evidence() {
    let output = run_json(&["live-fixture-run", "group-count", "metric"], true);

    assert!(output.contains("\"command\":\"live-fixture-run\""));
    assert!(output.contains(&field("mode", "live_fixture_run")));
    assert!(output.contains(&field("fixture_operator", "group_count")));
    assert!(output.contains(&field("input_change_record_count", "10")));
    assert!(output.contains(&field("active_state_key_count", "3")));
    assert!(output.contains(&field("output_row_count", "2")));
    assert!(output.contains(&field(
        "output_rows",
        "east:group_count:2|west:group_count:1"
    )));
    assert!(output.contains(&field("freshness_certificate_emitted", "true")));
    assert!(output.contains(&field("freshness_certificate_status", "certified")));
    assert!(output.contains(&field("state_certificate_emitted", "true")));
    assert!(output.contains(&field(
        "checkpoint_ref",
        "checkpoint://cg22/live/fixture/seq-10"
    )));
    assert!(output.contains(&field("continuous_view_certificate_emitted", "true")));
    assert!(output.contains(&field("execution_certificate_emitted", "true")));
    assert!(output.contains(&field("execution_certificate_status", "certified")));
    assert!(output.contains(&field("native_io_certificate_emitted", "true")));
    assert!(output.contains(&field("native_io_certificate_status", "certified")));
    assert!(output.contains(&field("runtime_execution", "true")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("broker_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn live_hybrid_state_transition_smoke_emits_retry_cancellation_cleanup_evidence() {
    let output = run_json(&["live-hybrid-state-transition-smoke"], true);

    assert!(output.contains("\"command\":\"live-hybrid-state-transition-smoke\""));
    assert!(output.contains(&field("mode", "live_hybrid_state_transition_smoke")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.live_hybrid_state_transition_fixture.v1"
    )));
    assert!(output.contains(&field(
        "fixture_id",
        "cg22.live_hybrid.state_transition.fixture.v1"
    )));
    assert!(output.contains(&field("selected_engine_mode", "hybrid")));
    assert!(output.contains(&field(
        "transition_kind",
        "bounded_snapshot_retry_cleanup_fixture"
    )));
    assert!(output.contains(&field("snapshot_epoch", "11")));
    assert!(output.contains(&field("input_change_record_count", "10")));
    assert!(output.contains(&field("active_state_key_count", "3")));
    assert!(output.contains(&field("freshness_certificate_status", "certified")));
    assert!(output.contains(&field("state_certificate_status", "certified")));
    assert!(output.contains(&field("state_transition_certificate_status", "certified")));
    assert!(output.contains(&field(
        "retry_policy",
        "single_retry_after_cooperative_cancellation"
    )));
    assert!(output.contains(&field("attempt_count", "2")));
    assert!(output.contains(&field(
        "attempt_outcome_order",
        "attempt-1:cancelled_cleanup_completed,attempt-2:certified"
    )));
    assert!(output.contains(&field("retry_performed", "true")));
    assert!(output.contains(&field("cancellation_requested", "true")));
    assert!(output.contains(&field("cancellation_cleanup_completed", "true")));
    assert!(output.contains(&field("partial_output_tracked", "true")));
    assert!(output.contains(&field("partial_output_committed", "false")));
    assert!(output.contains(&field("durable_checkpoint_store_used", "false")));
    assert!(output.contains(&field("durable_checkpoint_write_performed", "false")));
    assert!(output.contains(&field("broker_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("exactly_once_claim_allowed", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("no_fallback_no_external_engine", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
#[allow(clippy::too_many_lines)]
fn distributed_local_fixture_run_emits_worker_attempt_fragment_and_merge_evidence() {
    let output = run_json(
        &["distributed-local-fixture-run", "2", "fault-injection"],
        true,
    );

    assert!(output.contains("\"command\":\"distributed-local-fixture-run\""));
    assert!(output.contains(&field("mode", "distributed_local_fixture_run")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_distributed_fixture_run.v1"
    )));
    assert!(output.contains(&field(
        "distributed_runtime_status",
        "scoped_local_fixture_supported"
    )));
    assert!(output.contains(&field(
        "distributed_claim_gate_status",
        "not_distributed_runtime_grade"
    )));
    assert!(output.contains(&field("worker_count", "2")));
    assert!(output.contains(&field("local_worker_count", "2")));
    assert!(output.contains(&field("remote_worker_invoked", "false")));
    assert!(output.contains(&field("coordinator_invoked", "true")));
    assert!(output.contains(&field("local_worker_runtime_invoked", "true")));
    assert!(output.contains(&field("split_execution_performed", "true")));
    assert!(output.contains(&field("shuffle_repartition_performed", "true")));
    assert!(output.contains(&field("local_combine_performed", "true")));
    assert!(output.contains(&field("global_merge_performed", "true")));
    assert!(output.contains(&field("deterministic_merge_performed", "true")));
    assert!(output.contains(&field(
        "split_manifest_schema_version",
        "shardloom.local_distributed_split_manifest.v1"
    )));
    assert!(output.contains(&field("split_unit_count", "3")));
    assert!(output.contains(&field("split_id_order", "split-000,split-001,split-002")));
    assert!(output.contains(&field(
        "worker_assignment_order",
        "split-000:worker-00,split-001:worker-01,split-002:worker-00"
    )));
    assert!(output.contains(&field(
        "capillary_split_window",
        "bounded_three_split_fixture"
    )));
    assert!(output.contains(&field(
        "pulseweave_control_surface",
        "in_process_coordinator_worker_attempt_graph"
    )));
    assert!(output.contains(&field(
        "dynamic_admission_policy",
        "local_fixture_only_no_remote_workers"
    )));
    assert!(output.contains(&field(
        "shuffle_repartition_schema_version",
        "shardloom.local_distributed_shuffle_repartition.v1"
    )));
    assert!(output.contains(&field(
        "shuffle_repartition_strategy",
        "local_hash_group_key_to_reduce_worker"
    )));
    assert!(output.contains(&field(
        "local_combine_strategy",
        "split_local_group_count_sum_before_exchange"
    )));
    assert!(output.contains(&field(
        "global_merge_strategy",
        "partition_ordered_reduce_merge"
    )));
    assert!(output.contains(&field("reduce_partition_key", "group_key")));
    assert!(output.contains(&field("reduce_partition_count", "2")));
    assert!(output.contains(&field("repartition_performed", "true")));
    assert!(output.contains(&field("remote_shuffle_performed", "false")));
    assert!(output.contains(&field("raw_input_row_count", "7")));
    assert!(output.contains(&field("local_combined_row_count", "7")));
    assert!(output.contains(&field("global_merge_input_row_count", "7")));
    assert!(output.contains(&field(
        "skew_schema_version",
        "shardloom.local_distributed_skew.v1"
    )));
    assert!(output.contains(&field(
        "skew_detection_strategy",
        "group_count_threshold_after_local_combine"
    )));
    assert!(output.contains(&field("skew_detection_performed", "true")));
    assert!(output.contains(&field("skew_detected", "true")));
    assert!(output.contains(&field("skew_handling_applied", "true")));
    assert!(output.contains(&field("skew_threshold_rows", "3")));
    assert!(output.contains(&field("max_group_rows", "3")));
    assert!(output.contains(&field("skewed_group_key_order", "east")));
    assert!(output.contains(&field(
        "memory_backpressure_schema_version",
        "shardloom.local_distributed_memory_backpressure.v1"
    )));
    assert!(output.contains(&field("memory_budget_enforced", "true")));
    assert!(output.contains(&field("memory_budget_exceeded", "false")));
    assert!(output.contains(&field(
        "backpressure_policy",
        "bounded_worker_slots_and_reduce_partition_budget"
    )));
    assert!(output.contains(&field("backpressure_signal_emitted", "false")));
    assert!(output.contains(&field("spill_policy", "fail_closed_no_spill_for_fixture")));
    assert!(output.contains(&field("spill_required", "false")));
    assert!(output.contains(&field("production_spill_claim_allowed", "false")));
    assert!(output.contains(&field("task_attempt_count", "6")));
    assert!(output.contains("split-001:attempt-split-001-1:cancelled_cleanup_completed"));
    assert!(output.contains("split-002:attempt-split-002-2:duplicate_rejected"));
    assert!(output.contains("split-000:attempt-split-000-2:stale_lease_rejected"));
    assert!(output.contains(&field("result_fragment_count", "3")));
    assert!(output.contains(&field("merged_row_count", "3")));
    assert!(output.contains(&field("merged_rows", "east:3:13|north:2:10|west:2:9")));
    assert!(output.contains(&field("retry_performed", "true")));
    assert!(output.contains(&field("duplicate_attempt_rejected", "true")));
    assert!(output.contains(&field("stale_lease_rejected", "true")));
    assert!(output.contains(&field("cancellation_cleanup_completed", "true")));
    assert!(output.contains(&field("partial_output_committed", "false")));
    assert!(output.contains(&field("execution_certificate_status", "certified")));
    assert!(output.contains(&field("native_io_certificate_status", "certified")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("distributed_performance_claim_allowed", "false")));
    assert!(output.contains(&field("no_fallback_no_external_engine", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn hybrid_overlay_run_group_count_emits_overlay_flush_layout_and_certificate_evidence() {
    let output = run_json(&["hybrid-overlay-run", "group-count", "metric"], true);

    assert!(output.contains("\"command\":\"hybrid-overlay-run\""));
    assert!(output.contains(&field("mode", "hybrid_fixture_run")));
    assert!(output.contains(&field("fixture_operator", "group_count")));
    assert!(output.contains(&field("base_row_count", "4")));
    assert!(output.contains(&field("hot_change_record_count", "6")));
    assert!(output.contains(&field("hot_changelog_range", "1..6")));
    assert!(output.contains(&field("merged_row_count", "3")));
    assert!(output.contains(&field("output_row_count", "2")));
    assert!(output.contains(&field(
        "output_rows",
        "east:group_count:2|west:group_count:1"
    )));
    assert!(output.contains(&field("delta_overlay_certificate_emitted", "true")));
    assert!(output.contains(&field("delta_overlay_certificate_status", "certified")));
    assert!(output.contains(&field(
        "base_snapshot_certificate_id",
        "cg22.hybrid.fixture.base_snapshot"
    )));
    assert!(output.contains(&field(
        "merged_snapshot_certificate_id",
        "cg22.hybrid.fixture.merged_snapshot"
    )));
    assert!(output.contains(&field("base_snapshot_id", "snapshot://cg22/hybrid/base/v1")));
    assert!(output.contains(&field(
        "merged_snapshot_id",
        "snapshot://cg22/hybrid/merged/epoch-42"
    )));
    assert!(output.contains(&field("deletion_vector_entry_count", "2")));
    assert!(output.contains(&field("tombstone_count", "1")));
    assert!(output.contains(&field("hot_cold_contribution_report_emitted", "true")));
    assert!(output.contains(&field("cold_segment_count", "1")));
    assert!(output.contains(&field("warm_segment_count", "2")));
    assert!(output.contains(&field("hot_micro_segment_count", "1")));
    assert!(output.contains(&field("micro_segment_flush_evidence_emitted", "true")));
    assert!(output.contains(&field("micro_segment_flush_evidence_status", "certified")));
    assert!(output.contains(&field("representation_state", "vortex_encoded_planned")));
    assert!(output.contains(&field("micro_segment_flush_write_performed", "false")));
    assert!(output.contains(&field("layout_health_bundle_emitted", "true")));
    assert!(output.contains(&field(
        "layout_health_bundle_status",
        "compaction_recommended"
    )));
    assert!(output.contains(&field("tombstone_pressure", "true")));
    assert!(output.contains(&field("compaction_plan_emitted", "true")));
    assert!(output.contains(&field("compaction_execution_allowed", "false")));
    assert!(output.contains(&field("freshness_certificate_status", "certified")));
    assert!(output.contains(&field("execution_certificate_status", "certified")));
    assert!(output.contains(&field("native_io_certificate_status", "certified")));
    assert!(output.contains(&field("runtime_execution", "true")));
    assert!(output.contains(&field("base_vortex_read_performed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
