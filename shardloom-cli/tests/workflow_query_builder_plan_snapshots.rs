use std::process::Command;

fn run_command_json(args: &[&str], expect_success: bool) -> String {
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

#[test]
fn explain_json_preserves_report_only_no_fallback_boundaries() {
    let output = run_command_json(
        &[
            "explain",
            "read_vortex(orders.vortex) -> filter(gte:value:3) -> select(metric,value)",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"explain\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_only")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("materialization_boundary_reported", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn estimate_json_preserves_report_only_no_fallback_boundaries() {
    let output = run_command_json(
        &[
            "estimate",
            "read_csv(events.csv) -> filter(id > 0) -> limit(10)",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"estimate\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_only")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains("\"engine\":null"));
}

#[test]
fn certify_surfaces_json_remain_report_only_for_lazy_workflows() {
    let execution = run_command_json(&["execution-certificate-plan", "--format", "json"], true);
    assert!(execution.contains("\"command\":\"execution-certificate-plan\""));
    assert!(execution.contains(&field("certificate_evaluation_performed", "false")));
    assert!(execution.contains(&field("machine_readable_certificate_surface", "true")));
    assert!(execution.contains(&field("fallback_execution_allowed", "false")));

    let native_io = run_command_json(&["native-io-envelope-plan", "--format", "json"], true);
    assert!(native_io.contains("\"command\":\"native-io-envelope-plan\""));
    assert!(native_io.contains(&field("runtime_execution", "false")));
    assert!(native_io.contains(&field("data_read", "false")));
    assert!(native_io.contains(&field("per_path_certificate_required", "true")));
    assert!(native_io.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn workflow_unsupported_plan_json_covers_dataframe_gaps_without_effects() {
    let collect = run_command_json(
        &[
            "workflow-unsupported-plan",
            "collect",
            "read_csv(events.csv)",
            "none",
            "--format",
            "json",
        ],
        false,
    );
    let write = run_command_json(
        &[
            "workflow-unsupported-plan",
            "write-parquet",
            "read_csv(events.csv)",
            "out.parquet",
            "--format",
            "json",
        ],
        false,
    );
    let sql = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql",
            "read_vortex(orders.vortex)",
            "select * from orders",
            "--format",
            "json",
        ],
        false,
    );

    for output in [&collect, &write, &sql] {
        assert!(output.contains("\"command\":\"workflow-unsupported-plan\""));
        assert!(output.contains("\"status\":\"unsupported\""));
        assert!(output.contains(&field("mode", "workflow_unsupported_plan")));
        assert!(output.contains(&field(
            "schema_version",
            "shardloom.workflow_unsupported.v1"
        )));
        assert!(output.contains(&field("unsupported_status", "unsupported")));
        assert!(output.contains(&field("severity", "error")));
        assert!(output.contains(&field("plan_only", "true")));
        assert!(output.contains(&field("side_effect_free", "true")));
        assert!(output.contains(&field("execution", "not_performed")));
        assert!(output.contains(&field("query_execution", "false")));
        assert!(output.contains(&field("runtime_execution", "false")));
        assert!(output.contains(&field("data_read", "false")));
        assert!(output.contains(&field("data_materialized", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("no_runtime", "true")));
        assert!(output.contains(&field("no_fallback", "true")));
        assert!(output.contains(&field("no_effects", "true")));
        assert!(output.contains("\"attempted\":false"));
    }
    assert!(collect.contains(&field("workflow_operation", "collect")));
    assert!(collect.contains(&field(
        "blocker_id",
        "cg21.workflow.collect.materialization_unsupported"
    )));
    assert!(collect.contains(&field("materialization_required", "true")));
    assert!(collect.contains(&field("write_required", "false")));
    assert!(write.contains(&field("workflow_operation", "write_parquet")));
    assert!(write.contains(&field(
        "blocker_id",
        "cg21.workflow.write_parquet.compatibility_export_unsupported"
    )));
    assert!(write.contains(&field("write_required", "true")));
    assert!(sql.contains(&field("workflow_operation", "sql")));
    assert!(sql.contains(&field(
        "blocker_id",
        "cg21.workflow.sql.frontend_unsupported"
    )));
    assert!(sql.contains("\"code\":\"SL_UNSUPPORTED_SQL\""));
}
