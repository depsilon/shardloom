use std::process::Command;

fn run_global_architecture_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["global-architecture-gate", "--format", "json"])
        .output()
        .expect("global-architecture-gate command runs");

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

#[test]
fn global_architecture_gate_exposes_claim_families_and_gate_refs() {
    let output = run_global_architecture_gate_json();

    assert!(output.contains("\"command\":\"global-architecture-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "global_architecture_runtime_claim_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.global_architecture_runtime_claim_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "gar0001a.global_architecture_runtime_claim_gate"
    )));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field("row_count", "10")));
    assert!(output.contains(&field(
        "claim_families",
        "distributed,object_store,lakehouse"
    )));
    assert!(output.contains(&field(
        "row_order",
        "distributed_coordinator_startup,distributed_worker_startup,distributed_task_execution,object_store_range_read,object_store_full_file_read,object_store_write,object_store_commit,lakehouse_catalog_metadata,lakehouse_transaction_commit,cdc_delete_tombstone_execution"
    )));
    assert!(output.contains(&field(
        "existing_gate_refs",
        "cg9.catalog_metadata_integration_gate,cg10.object_store_runtime_promotion_gate,cg10.object_store_request_planner.aggregate,shardloom.object_store_commit_protocol.v1,shardloom.table_compatibility.v1,table-compat-plan delete-semantics"
    )));
}

#[test]
fn global_architecture_gate_blocks_runtime_io_and_public_claims() {
    let output = run_global_architecture_gate_json();

    for key in [
        "runtime_claim_allowed",
        "distributed_runtime_claim_allowed",
        "object_store_runtime_claim_allowed",
        "lakehouse_runtime_claim_allowed",
        "public_claim_allowed",
        "coordinator_worker_start_allowed",
        "task_execution_allowed",
        "credential_resolution_allowed",
        "object_store_io_allowed",
        "table_catalog_io_allowed",
        "lakehouse_commit_allowed",
        "data_read_allowed",
        "write_io_allowed",
        "fallback_attempted",
        "fallback_execution_allowed",
        "external_engine_invoked",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "release_gate_required",
        "all_rows_side_effect_free",
        "all_rows_not_claim_grade",
        "all_runtime_claims_blocked",
        "deterministic_diagnostics_present",
        "side_effect_free",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains("SL_BLOCKED_DISTRIBUTED_RUNTIME"));
    assert!(output.contains("SL_BLOCKED_OBJECT_STORE_COMMIT"));
    assert!(output.contains("SL_BLOCKED_LAKEHOUSE_TRANSACTION_COMMIT"));
}
