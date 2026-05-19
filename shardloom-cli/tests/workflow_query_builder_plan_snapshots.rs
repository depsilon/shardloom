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
#[allow(clippy::too_many_lines)]
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
    let from_pandas = run_command_json(
        &[
            "workflow-unsupported-plan",
            "from-pandas",
            "read_pandas(pandas:DataFrame)",
            "pandas:DataFrame",
            "--format",
            "json",
        ],
        false,
    );
    let to_numpy = run_command_json(
        &[
            "workflow-unsupported-plan",
            "to-numpy",
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
    let with_column = run_command_json(
        &[
            "workflow-unsupported-plan",
            "with-column",
            "read_csv(events.csv)",
            "date=to_date(ts)",
            "--format",
            "json",
        ],
        false,
    );
    let group_by = run_command_json(
        &[
            "workflow-unsupported-plan",
            "group-by",
            "read_csv(events.csv)",
            "customer_id",
            "--format",
            "json",
        ],
        false,
    );
    let agg = run_command_json(
        &[
            "workflow-unsupported-plan",
            "agg",
            "read_csv(events.csv) -> group_by(customer_id)",
            "sum(amount)",
            "--format",
            "json",
        ],
        false,
    );
    let sort = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sort",
            "read_csv(events.csv)",
            "amount desc",
            "--format",
            "json",
        ],
        false,
    );
    let limit = run_command_json(
        &[
            "workflow-unsupported-plan",
            "limit",
            "read_csv(events.csv)",
            "10",
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
    let sql_parse = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql-parse",
            "sql(statement)",
            "select * from orders",
            "--format",
            "json",
        ],
        false,
    );
    let sql_bind = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql-bind",
            "sql(statement)",
            "select * from orders",
            "--format",
            "json",
        ],
        false,
    );
    let sql_plan = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql-plan",
            "sql(statement)",
            "select * from orders",
            "--format",
            "json",
        ],
        false,
    );
    let sql_execute = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql-execute",
            "sql(statement)",
            "select * from orders",
            "--format",
            "json",
        ],
        false,
    );
    let sql_source_free_projection = run_command_json(
        &[
            "workflow-unsupported-plan",
            "sql-source-free-projection",
            "source_free(sql_source_free_projection)",
            "SELECT 1 AS value",
            "--format",
            "json",
        ],
        false,
    );
    let dataframe_source_free_projection = run_command_json(
        &[
            "workflow-unsupported-plan",
            "dataframe-source-free-projection",
            "source_free(dataframe_source_free_projection)",
            "lit(1).alias(value)",
            "--format",
            "json",
        ],
        false,
    );
    let dataframe_generated_with_column = run_command_json(
        &[
            "workflow-unsupported-plan",
            "dataframe-generated-with-column",
            "source_free(dataframe_generated_with_column)",
            "value=lit(1)",
            "--format",
            "json",
        ],
        false,
    );
    let object_store_generated_output = run_command_json(
        &[
            "workflow-unsupported-plan",
            "object-store-generated-output",
            "source_free(object_store_generated_output)",
            "s3://bucket/out.jsonl",
            "--format",
            "json",
        ],
        false,
    );
    let foundry_generated_output = run_command_json(
        &[
            "workflow-unsupported-plan",
            "foundry-generated-output",
            "source_free(foundry_generated_output)",
            "foundry://dataset/output",
            "--format",
            "json",
        ],
        false,
    );
    let schema = run_command_json(
        &[
            "workflow-unsupported-plan",
            "describe-schema",
            "read_csv(events.csv)",
            "none",
            "--format",
            "json",
        ],
        false,
    );
    let preview = run_command_json(
        &[
            "workflow-unsupported-plan",
            "preview",
            "read_vortex(orders.vortex)",
            "20",
            "--format",
            "json",
        ],
        false,
    );
    let object_store_read = run_command_json(
        &[
            "workflow-unsupported-plan",
            "object-store-read",
            "read_s3(s3://bucket/table.vortex)",
            "s3://bucket/table.vortex",
            "--format",
            "json",
        ],
        false,
    );
    let fallback_engine = run_command_json(
        &[
            "workflow-unsupported-plan",
            "fallback-engine",
            "read_csv(events.csv)",
            "spark",
            "--format",
            "json",
        ],
        false,
    );

    for output in [
        &collect,
        &from_pandas,
        &to_numpy,
        &write,
        &with_column,
        &group_by,
        &agg,
        &sort,
        &limit,
        &sql,
        &sql_parse,
        &sql_bind,
        &sql_plan,
        &sql_execute,
        &sql_source_free_projection,
        &dataframe_source_free_projection,
        &dataframe_generated_with_column,
        &object_store_generated_output,
        &foundry_generated_output,
        &schema,
        &preview,
        &object_store_read,
        &fallback_engine,
    ] {
        assert!(output.contains("\"command\":\"workflow-unsupported-plan\""));
        assert!(output.contains("\"status\":\"unsupported\""));
        assert!(output.contains(&field("mode", "workflow_unsupported_plan")));
        assert!(output.contains(&field(
            "schema_version",
            "shardloom.workflow_unsupported.v1"
        )));
        assert!(output.contains(&field("support_status", "unsupported")));
        assert!(output.contains(&field("unsupported_status", "unsupported")));
        assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
        assert!(output.contains(&field("severity", "error")));
        assert!(output.contains(&field("plan_only", "true")));
        assert!(output.contains(&field("side_effect_free", "true")));
        assert!(output.contains(&field("execution", "not_performed")));
        assert!(output.contains(&field("parser_executed", "false")));
        assert!(output.contains(&field("binder_executed", "false")));
        assert!(output.contains(&field("planner_executed", "false")));
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
    assert!(collect.contains("\"code\":\"SL_MATERIALIZATION_REQUIRED\""));
    assert!(collect.contains("\"category\":\"materialization\""));
    assert!(collect.contains(&field("diagnostic_code", "SL_MATERIALIZATION_REQUIRED")));
    assert!(collect.contains(&field("diagnostic_category", "materialization")));
    assert!(collect.contains(&field("materialization_required", "true")));
    assert!(collect.contains(&field("write_required", "false")));
    assert!(from_pandas.contains(&field("workflow_operation", "from_pandas")));
    assert!(from_pandas.contains(&field(
        "blocker_id",
        "cg21.workflow.from_pandas.materialized_input_unsupported"
    )));
    assert!(from_pandas.contains(&field("runtime_required", "false")));
    assert!(to_numpy.contains(&field("workflow_operation", "to_numpy")));
    assert!(to_numpy.contains(&field(
        "blocker_id",
        "cg21.workflow.to_numpy.python_array_unsupported"
    )));
    assert!(write.contains(&field("workflow_operation", "write_parquet")));
    assert!(write.contains(&field(
        "blocker_id",
        "cg21.workflow.write_parquet.compatibility_export_unsupported"
    )));
    assert!(write.contains(&field("write_required", "true")));
    assert!(with_column.contains(&field("workflow_operation", "with_column")));
    assert!(with_column.contains(&field(
        "blocker_id",
        "cg21.workflow.with_column.expression_unsupported"
    )));
    assert!(group_by.contains(&field("workflow_operation", "group_by")));
    assert!(group_by.contains(&field(
        "blocker_id",
        "cg21.workflow.group_by.operator_unsupported"
    )));
    assert!(agg.contains(&field("workflow_operation", "agg")));
    assert!(agg.contains(&field(
        "blocker_id",
        "cg21.workflow.agg.operator_unsupported"
    )));
    assert!(sort.contains(&field("workflow_operation", "sort")));
    assert!(sort.contains(&field(
        "blocker_id",
        "cg21.workflow.sort.operator_unsupported"
    )));
    assert!(limit.contains(&field("workflow_operation", "limit")));
    assert!(limit.contains(&field(
        "blocker_id",
        "cg21.workflow.limit.execution_uncertified"
    )));
    assert!(sql.contains(&field("workflow_operation", "sql")));
    assert!(sql.contains(&field(
        "blocker_id",
        "cg21.workflow.sql.frontend_unsupported"
    )));
    assert!(sql.contains("\"code\":\"SL_UNSUPPORTED_SQL\""));
    assert!(sql_parse.contains(&field("workflow_operation", "sql_parse")));
    assert!(sql_parse.contains(&field("blocker_id", "cg21.workflow.sql.parse_unsupported")));
    assert!(sql_parse.contains(&field("runtime_required", "false")));
    assert!(sql_bind.contains(&field("workflow_operation", "sql_bind")));
    assert!(sql_plan.contains(&field("workflow_operation", "sql_plan")));
    assert!(sql_execute.contains(&field("workflow_operation", "sql_execute")));
    assert!(sql_execute.contains(&field("runtime_required", "true")));
    assert!(
        sql_source_free_projection
            .contains(&field("workflow_operation", "sql_source_free_projection"))
    );
    assert!(sql_source_free_projection.contains(&field(
        "blocker_id",
        "gar-gen-1.sql_source_free_projection_runtime_not_implemented"
    )));
    assert!(sql_source_free_projection.contains("\"code\":\"SL_UNSUPPORTED_SQL\""));
    assert!(dataframe_source_free_projection.contains(&field(
        "workflow_operation",
        "dataframe_source_free_projection"
    )));
    assert!(dataframe_source_free_projection.contains(&field(
        "blocker_id",
        "gar-gen-1.dataframe_source_free_projection_runtime_not_implemented"
    )));
    assert!(dataframe_generated_with_column.contains(&field(
        "workflow_operation",
        "dataframe_generated_with_column"
    )));
    assert!(dataframe_generated_with_column.contains(&field(
        "blocker_id",
        "gar-gen-1.dataframe_generated_with_column_runtime_not_implemented"
    )));
    assert!(object_store_generated_output.contains(&field(
        "workflow_operation",
        "object_store_generated_output"
    )));
    assert!(object_store_generated_output.contains(&field(
        "blocker_id",
        "gar-gen-1.object_store_generated_output_blocked"
    )));
    assert!(object_store_generated_output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(object_store_generated_output.contains(&field("write_required", "true")));
    assert!(
        foundry_generated_output.contains(&field("workflow_operation", "foundry_generated_output"))
    );
    assert!(foundry_generated_output.contains(&field(
        "blocker_id",
        "gar-gen-1.foundry_generated_output_runtime_not_implemented"
    )));
    assert!(foundry_generated_output.contains(&field("write_required", "true")));
    assert!(schema.contains(&field("workflow_operation", "describe_schema")));
    assert!(schema.contains(&field(
        "blocker_id",
        "cg21.workflow.describe_schema.report_unsupported"
    )));
    assert!(schema.contains(&field("runtime_required", "false")));
    assert!(preview.contains(&field("workflow_operation", "preview")));
    assert!(preview.contains(&field(
        "blocker_id",
        "cg21.workflow.preview.materialization_unsupported"
    )));
    assert!(preview.contains(&field("target_ref", "20")));
    assert!(object_store_read.contains(&field("workflow_operation", "object_store_read")));
    assert!(object_store_read.contains(&field(
        "blocker_id",
        "cg21.workflow.object_store_read.runtime_unsupported"
    )));
    assert!(object_store_read.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(object_store_read.contains("\"category\":\"object_store\""));
    assert!(object_store_read.contains(&field("diagnostic_category", "object_store")));
    assert!(object_store_read.contains(&field("object_store_io", "false")));
    assert!(fallback_engine.contains(&field("workflow_operation", "fallback_engine")));
    assert!(fallback_engine.contains(&field(
        "blocker_id",
        "cg21.workflow.fallback_engine.no_fallback_policy"
    )));
    assert!(fallback_engine.contains("\"code\":\"SL_NO_FALLBACK_EXECUTION\""));
    assert!(fallback_engine.contains("\"category\":\"no_fallback_policy\""));
    assert!(fallback_engine.contains(
        "external fallback engine workflow execution is prohibited by ShardLoom's no-fallback policy"
    ));
    assert!(fallback_engine.contains(&field("diagnostic_category", "no_fallback_policy")));
    assert!(fallback_engine.contains(&field("runtime_required", "false")));
}

#[test]
fn workflow_unsupported_plan_unknown_operation_uses_invalid_input_category() {
    let output = run_command_json(
        &[
            "workflow-unsupported-plan",
            "unknown-op",
            "read_csv(events.csv)",
            "none",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"workflow-unsupported-plan\""));
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("\"code\":\"SL_INVALID_INPUT\""));
    assert!(output.contains("\"category\":\"invalid_input\""));
    assert!(output.contains("\"attempted\":false"));
}
