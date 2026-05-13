use std::process::Command;

fn run_dossier_json(args: &[&str], expect_success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

    assert_eq!(
        output.status.success(),
        expect_success,
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

fn assert_no_runtime_no_fallback_no_effects(output: &str) {
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("no_runtime", "true")));
    assert!(output.contains(&field("no_fallback", "true")));
    assert!(output.contains(&field("no_effects", "true")));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
}

#[test]
fn local_vortex_count_dossier_indexes_cross_cg_evidence_without_effects() {
    let output = run_dossier_json(
        &[
            "workload-certification-dossier",
            "local-vortex-count",
            "--format",
            "json",
        ],
        true,
    );

    assert!(output.contains("\"command\":\"workload-certification-dossier\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "workload_certification_dossier")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.workload_certification_dossier.v1"
    )));
    assert!(output.contains(&field("scenario", "local-vortex-count")));
    assert!(output.contains(&field("workload_id", "workload://cg7/local-vortex-count")));
    assert!(output.contains(&field("overall_status", "partial")));
    assert!(output.contains(&field(
        "status_vocabulary",
        "certified,partial,planned,report_only,blocked,unsupported"
    )));
    assert!(output.contains(&field("claim_allowed", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("cg5_correctness_status", "certified")));
    assert!(output.contains(&field("cg6_benchmark_status", "blocked")));
    assert!(output.contains(&field("cg16_execution_certificate_status", "certified")));
    assert!(output.contains(&field("cg19_native_io_certificate_status", "certified")));
    assert!(output.contains(&field("cg20_capability_evidence_status", "report_only")));
    assert!(output.contains(&field("cg21_workflow_evidence_status", "report_only")));
    assert!(output.contains(&field("cg22_engine_evidence_status", "partial")));
    assert!(output.contains(&field("cg23_api_evidence_status", "planned")));
    assert!(output.contains(&field(
        "certificate_refs",
        "certificates/cg16/local-vortex-count/execution.json,certificates/cg19/local-vortex-count/native-io.json"
    )));
    assert!(output.contains(&field(
        "missing_evidence",
        "claim_grade_benchmark_results,api_contract_workload_mapping"
    )));
    assert!(output.contains(&field(
        "blocker_ids",
        "cg6.benchmark.claim_grade_results_missing,cg23.api.workload_mapping_planned"
    )));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn planned_live_hybrid_dossier_keeps_certificate_refs_empty_until_evidence_exists() {
    let output = run_dossier_json(
        &[
            "workload-certification-dossier",
            "planned-live-hybrid",
            "--format",
            "json",
        ],
        true,
    );

    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("overall_status", "planned")));
    assert!(output.contains(&field("certificate_refs", "none")));
    assert!(output.contains(&field(
        "missing_evidence",
        "state_certificate,durable_checkpoint_store,benchmark_evidence,api_event_stream_certificate"
    )));
    assert!(output.contains(&field(
        "blocker_ids",
        "cg22.engine.live.durable_checkpoint_store,cg22.engine.hybrid.object_store_commit_protocol"
    )));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn blocked_remote_api_dossier_returns_unsupported_problem_without_effects() {
    let output = run_dossier_json(
        &[
            "workload-certification-dossier",
            "blocked-remote-api",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("overall_status", "blocked")));
    assert!(output.contains(&field("certificate_refs", "none")));
    assert!(output.contains(&field(
        "missing_evidence",
        "object_store_certificate,remote_execution_policy,native_io_certificate,execution_certificate"
    )));
    assert!(output.contains(&field(
        "blocked_evidence",
        "cg23.remote_api.remote_object_store.unsupported,cg19.native_io.remote_object_store_certificate_missing"
    )));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn unsupported_sql_dossier_returns_unsupported_sql_without_effects() {
    let output = run_dossier_json(
        &[
            "workload-certification-dossier",
            "unsupported-sql",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("overall_status", "unsupported")));
    assert!(output.contains(&field("certificate_refs", "none")));
    assert!(output.contains(&field(
        "missing_evidence",
        "sql_parser,binder,semantic_profile,operator_capability_matrix,execution_certificate,native_io_certificate"
    )));
    assert!(output.contains(&field("unsupported_evidence", "sql_frontend")));
    assert!(output.contains(&field(
        "blocker_ids",
        "cg21.workflow.sql.frontend_unsupported,cg23.remote_api.plan_preview.unsupported_operator"
    )));
    assert!(output.contains("\"code\":\"SL_UNSUPPORTED_SQL\""));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn default_dossier_matches_local_vortex_count_certificate_refs() {
    let output = run_dossier_json(
        &["workload-certification-dossier", "--format", "json"],
        true,
    );

    assert!(output.contains(&field("scenario", "local-vortex-count")));
    assert!(output.contains("certificates/cg16/local-vortex-count/execution.json"));
    assert!(output.contains("certificates/cg19/local-vortex-count/native-io.json"));
    assert!(!output.contains(&field("certificate_refs", "none")));
}
