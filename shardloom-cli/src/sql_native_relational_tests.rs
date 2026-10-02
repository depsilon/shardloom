use super::*;
use serde_json::{Value, json};
use shardloom_exec::compute_pool::CancellationToken;

fn fixture() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

fn verify(statement: &str, expected: &Value) {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let prepared = prepare(statement, policy, |path| {
        DatasetUri::new(path.to_string_lossy().into_owned())
    })
    .unwrap_or_else(|error| panic!("{statement}: {error}"));
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
        let rows = result
            .result_jsonl
            .value()
            .lines()
            .map(|row| serde_json::from_str::<Value>(row).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(json!(rows), *expected, "{statement}");
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        assert_eq!(result.execution.runtime.completed_executions, execution);
    }
}

#[test]
fn native_relational_sql_join_predicates_and_grouped_having_execute_shared_native_nodes() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value, r.value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4 LIMIT 20"
        ),
        &json!([
            {"l.value":4,"r.value":5},{"l.value":5,"r.value":null}
        ]),
    );
    verify(
        &format!(
            "SELECT l.value, COUNT(*) AS rows FROM '{source}' AS l JOIN '{source}' AS r ON l.value < r.value GROUP BY l.value HAVING COUNT(*) > 2 ORDER BY rows DESC LIMIT 20"
        ),
        &json!([
            {"l.value":1,"rows":4},{"l.value":2,"rows":3}
        ]),
    );
    verify(
        &format!(
            "SELECT l.value FROM '{source}' AS l LEFT ANTI JOIN '{source}' AS r ON l.value < r.value LIMIT 20"
        ),
        &json!([{"l.value":5}]),
    );
}

#[test]
fn native_relational_sql_literal_on_precedes_full_join_null_extension() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value,r.value FROM '{source}' AS l FULL JOIN '{source}' AS r ON l.value = r.value AND l.value > 3"
        ),
        &json!([
            {"l.value":1,"r.value":null},{"l.value":2,"r.value":null},
            {"l.value":3,"r.value":null},{"l.value":4,"r.value":4},
            {"l.value":5,"r.value":5},{"l.value":null,"r.value":1},
            {"l.value":null,"r.value":2},{"l.value":null,"r.value":3}
        ]),
    );
}

#[test]
fn native_relational_sql_extracts_only_unconditional_cross_input_equality_keys() {
    for (condition, expected) in [
        (
            "left.value = right.value AND left.value > 3",
            vec![("value", "value")],
        ),
        (
            "right.value = left.value AND right.value > 3",
            vec![("value", "value")],
        ),
        (
            "left.value = right.value AND (left.other = right.other OR left.value > 3)",
            vec![("value", "value")],
        ),
        ("left.value = right.value OR left.value > 3", vec![]),
        ("left.value = left.other AND left.value > 3", vec![]),
        (
            "left.value = right.value AND right.value = left.value AND left.other = right.other AND left.value > 3",
            vec![("value", "value"), ("other", "other")],
        ),
    ] {
        let sql_condition = condition.replace("left.", "l.").replace("right.", "r.");
        let parsed = parse_sql_local_source_statement(&format!(
            "SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON {sql_condition} LIMIT 10"
        )).unwrap();
        let mut expression = parsed
            .join
            .unwrap()
            .on_predicate
            .unwrap()
            .to_expression()
            .unwrap();
        map_columns(&mut expression, &mut |name| {
            Ok(name.replacen("l.", "left.", 1).replacen("r.", "right.", 1))
        })
        .unwrap();
        let mut keys = Vec::new();
        append_equality_keys(&expression, &mut keys).unwrap();
        assert_eq!(
            keys.iter()
                .map(|key| (key.left.as_str(), key.right.as_str()))
                .collect::<Vec<_>>(),
            expected,
            "{condition}"
        );
    }
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value,r.value FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value OR l.value = 1"
        ),
        &json!([
            {"l.value":1,"r.value":1},{"l.value":1,"r.value":2},
            {"l.value":1,"r.value":3},{"l.value":1,"r.value":4},
            {"l.value":1,"r.value":5},{"l.value":2,"r.value":2},
            {"l.value":3,"r.value":3},{"l.value":4,"r.value":4},
            {"l.value":5,"r.value":5}
        ]),
    );
}

#[test]
fn native_relational_sql_column_aliases_preserve_source_values_without_arithmetic() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value AS left_value,r.value AS right_value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4"
        ),
        &json!([{"left_value":4,"right_value":5},{"left_value":5,"right_value":null}]),
    );
}

#[test]
fn native_relational_sql_scalar_aggregate_subquery_needs_no_grouping_or_having() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT COUNT(*) AS n FROM '{source}' WHERE value <= outer.value)"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_limit_admission_does_not_expand_reference_materialization() {
    let source = fixture();
    let statement = format!(
        "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' LIMIT 64) LIMIT 10"
    );
    verify(
        &statement,
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    let parsed = parse_sql_local_source_statement(&statement).unwrap();
    let ParsedPredicate::InSubquery { mut subquery, .. } = parsed.predicate else {
        panic!("expected IN");
    };
    let rejected = materialize_in_subquery(
        &mut subquery,
        None,
        SqlLocalSourceRuntimeProfile::Smoke.read_limits(),
    )
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("decoded reference subquery LIMIT")
    );
}

#[test]
fn native_relational_sql_exists_binds_selected_columns_before_reducing_to_presence() {
    let source = fixture();
    let statement =
        format!("SELECT value FROM '{source}' WHERE EXISTS (SELECT absent_column FROM '{source}')");
    let result = prepare(
        &statement,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.to_string_lossy().into_owned()),
    );
    assert!(result.is_err());
    assert!(result.err().unwrap().to_string().contains("absent_column"));
}

#[test]
fn native_relational_sql_sets_align_names_preserve_multiplicity_and_apply_only_global_limit() {
    let source = fixture();
    for (operator, expected) in [
        (
            "UNION ALL",
            json!([{"value":1},{"value":2},{"value":3},{"value":3},{"value":4},{"value":5}]),
        ),
        (
            "UNION",
            json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
        ),
        ("INTERSECT", json!([{"value":3}])),
        ("EXCEPT", json!([{"value":1},{"value":2}])),
    ] {
        verify(
            &format!(
                "SELECT value FROM '{source}' WHERE value <= 3 {operator} SELECT value + 0 AS renamed FROM '{source}' WHERE value >= 3 ORDER BY value ASC LIMIT 20"
            ),
            &expected,
        );
    }
    verify(
        &format!(
            "SELECT DISTINCT l.value FROM '{source}' AS l JOIN '{source}' AS r ON l.value < r.value ORDER BY l.value DESC LIMIT 2"
        ),
        &json!([{"l.value":4},{"l.value":3}]),
    );
}

#[test]
fn native_relational_sql_windows_keep_input_order_and_can_order_by_the_result_alias() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value, ROW_NUMBER() OVER (ORDER BY value DESC) AS position FROM '{source}' LIMIT 10"
        ),
        &json!([
            {"value":1,"position":5},{"value":2,"position":4},{"value":3,"position":3},{"value":4,"position":2},{"value":5,"position":1}
        ]),
    );
    verify(
        &format!(
            "SELECT value, LEAD(value, 2) OVER (ORDER BY value ASC) AS next FROM '{source}' ORDER BY next ASC NULLS LAST LIMIT 10"
        ),
        &json!([
            {"value":1,"next":3},{"value":2,"next":4},{"value":3,"next":5},{"value":4,"next":null},{"value":5,"next":null}
        ]),
    );
}

#[test]
fn native_relational_sql_subqueries_lower_relations_instead_of_unfilled_value_caches() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' WHERE value > 3) LIMIT 10"
        ),
        &json!([{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value > ALL (SELECT value FROM '{source}' WHERE value < 3) LIMIT 10"
        ),
        &json!([{"value":3},{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE EXISTS (SELECT 1 FROM '{source}' WHERE value > outer.value) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' WHERE value <= outer.value ORDER BY value DESC LIMIT 1) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_projected_correlated_aggregate_and_predicate_projection_compose() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT COUNT(*) AS n FROM '{source}' WHERE value <= outer.value HAVING COUNT(*) >= 1) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value, CASE WHEN value IN (SELECT value FROM '{source}' WHERE value > 3) THEN 'yes' ELSE 'no' END AS membership FROM '{source}' LIMIT 10"
        ),
        &json!([
            {"value":1,"membership":"no"},{"value":2,"membership":"no"},{"value":3,"membership":"no"},{"value":4,"membership":"yes"},{"value":5,"membership":"yes"}
        ]),
    );
}

#[test]
fn native_relational_sql_shape_inspection_does_not_open_missing_sources() {
    assert!(is_relational("SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON l.value = r.value").unwrap());
    assert!(is_relational("SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/right.vortex'").unwrap());
    assert!(!is_relational("SELECT value FROM '/missing/left.vortex' LIMIT 1").unwrap());
    assert_eq!(source_count("SELECT value FROM '/missing/left.csv' WHERE value IN (SELECT value FROM '/missing/right.vortex' WHERE value IN (SELECT value FROM '/missing/nested.jsonl'))").unwrap(), 3);
    assert_eq!(source_count("SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/right.vortex'").unwrap(), 2);
    assert_eq!(source_count("SELECT CASE WHEN EXISTS (SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON l.value = r.value) THEN 1 ELSE 0 END AS present FROM '/missing/outer.vortex'").unwrap(), 3);
}
