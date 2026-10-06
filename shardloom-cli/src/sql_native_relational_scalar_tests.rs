use super::memory_tests::verify_memory;
use super::*;

#[test]
fn native_scalar_subqueries_preserve_cardinality_types_and_expression_composition() {
    for (sql, expected) in [
        ("SELECT (SELECT 9) AS scalar", json!([{"scalar":9}])),
        ("SELECT (SELECT NULL) AS scalar", json!([{"scalar":null}])),
        (
            "SELECT (SELECT value FROM range(4,4)) AS scalar FROM range(1,3)",
            json!([{"scalar":null},{"scalar":null}]),
        ),
        (
            "SELECT value + (SELECT value FROM range(5,6)) AS scalar FROM range(1,3)",
            json!([{"scalar":6},{"scalar":7}]),
        ),
        (
            "SELECT CAST((SELECT CAST('1.20' AS decimal128(4,2))) AS utf8) AS scalar",
            json!([{"scalar":"1.20"}]),
        ),
        (
            "SELECT (SELECT ARRAY[1,2,NULL] AS items) AS scalar",
            json!([{"scalar":[1,2,null]}]),
        ),
        (
            "SELECT (SELECT STRUCT(value) AS payload FROM range(7,8)) AS scalar",
            json!([{"scalar":{"value":7}}]),
        ),
        (
            "SELECT value FROM range(1,4) WHERE value < (SELECT 3)",
            json!([{"value":1},{"value":2}]),
        ),
        (
            "SELECT value FROM range(1,3) WHERE (SELECT value FROM range(1,1)) IS NULL",
            json!([{"value":1},{"value":2}]),
        ),
        (
            "SELECT SUM(value + (SELECT 2)) AS total FROM range(1,4)",
            json!([{"total":12.0}]),
        ),
        (
            "SELECT COUNT(DISTINCT (SELECT 7)) AS n,COUNT(DISTINCT (value+1)) AS distinct_values FROM range(1,4)",
            json!([{"n":1,"distinct_values":3}]),
        ),
        (
            "SELECT FIRST_VALUE(value + (SELECT 10)) OVER (ORDER BY value) AS first FROM range(1,3)",
            json!([{"first":11},{"first":11}]),
        ),
        (
            "SELECT (SELECT value FROM (SELECT value FROM range(3,4) UNION ALL SELECT value FROM range(1,2)) AS u ORDER BY value LIMIT 1) AS scalar",
            json!([{"scalar":1}]),
        ),
        (
            "SELECT (SELECT DISTINCT column_1 FROM (VALUES (4),(4)) AS v) AS scalar",
            json!([{"scalar":4}]),
        ),
        (
            "SELECT COALESCE((SELECT MAX(value) AS best FROM range(1,5) WHERE value <= 2 AND value > 0),(SELECT 9)) AS scalar",
            json!([{"scalar":2}]),
        ),
        (
            "SELECT (SELECT 7) AS grouped,COUNT(*) AS n FROM range(1,4) GROUP BY grouped",
            json!([{"grouped":7,"n":3}]),
        ),
        (
            "SELECT COUNT(*) AS n FROM range(1,4) HAVING COUNT(*)=(SELECT 3)",
            json!([{"n":3}]),
        ),
        (
            "WITH one AS (SELECT value FROM range(8,9)) SELECT (SELECT value FROM one) AS scalar",
            json!([{"scalar":8}]),
        ),
    ] {
        assert!(is_relational(sql).unwrap(), "{sql}");
        verify_memory(sql, &expected);
    }
}

#[test]
fn native_scalar_subqueries_preserve_selected_case_and_coalesce_demand() {
    for (sql, expected) in [
        (
            "SELECT CASE WHEN value=1 THEN (SELECT 7) ELSE (SELECT 8) END AS scalar FROM range(1,3)",
            json!([{"scalar":7},{"scalar":8}]),
        ),
        (
            "SELECT CASE WHEN TRUE THEN (SELECT 7) ELSE (SELECT value FROM range(1,3)) END AS scalar",
            json!([{"scalar":7}]),
        ),
        (
            "SELECT CASE WHEN FALSE THEN (SELECT 1/0 AS bad) ELSE (SELECT 8) END AS scalar",
            json!([{"scalar":8}]),
        ),
        (
            "SELECT COALESCE((SELECT 7),(SELECT value FROM range(1,3))) AS scalar",
            json!([{"scalar":7}]),
        ),
        (
            "SELECT COALESCE((SELECT value FROM range(1,1)),(SELECT 8)) AS scalar",
            json!([{"scalar":8}]),
        ),
        (
            "SELECT CASE WHEN (SELECT 1)=1 THEN (SELECT 7) ELSE (SELECT 1/0 AS bad) END AS scalar",
            json!([{"scalar":7}]),
        ),
        (
            "SELECT CASE WHEN TRUE THEN 7 ELSE (CASE WHEN EXISTS (SELECT 1/0 AS bad FROM range(1,2)) THEN (SELECT 8) ELSE (SELECT 9) END) END AS scalar",
            json!([{"scalar":7}]),
        ),
        (
            "SELECT CASE WHEN value=1 THEN (SELECT value FROM range(5,6)) ELSE (SELECT value FROM range(8,9)) END AS scalar FROM range(1,1)",
            json!([]),
        ),
    ] {
        verify_memory(sql, &expected);
    }
}

#[test]
fn native_scalar_subqueries_bind_every_branch_and_fail_on_a_second_row() {
    for sql in [
        "SELECT (SELECT value FROM range(1,3)) AS scalar",
        "SELECT (SELECT column_1 FROM (VALUES (4),(4)) AS v) AS scalar",
        "SELECT COALESCE((SELECT value FROM range(1,1)),(SELECT value FROM range(1,3))) AS scalar",
        "SELECT CASE WHEN FALSE THEN 7 ELSE (SELECT value FROM range(1,3)) END AS scalar",
    ] {
        let prepared = prepare(
            sql,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |_| panic!("memory only"),
        )
        .unwrap();
        let error = prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .expect("multiple rows must fail");
        assert!(
            error.to_string().contains("scalar subquery cardinality"),
            "{sql}: {error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
    for sql in [
        "SELECT (SELECT value,value AS other FROM range(1,2)) AS scalar FROM range(1,1)",
        "SELECT CASE WHEN TRUE THEN 7 ELSE (SELECT absent FROM range(1,2)) END AS scalar",
        "SELECT COALESCE((SELECT 7),(SELECT value,value AS other FROM range(1,2))) AS scalar",
    ] {
        assert!(
            prepare(
                sql,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                |_| panic!("memory only")
            )
            .is_err(),
            "{sql}"
        );
    }
}

#[test]
fn native_scalar_subqueries_reject_dynamic_schemas_before_input_preparation() {
    let options = r#"{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}"#;
    let pivot = format!(
        "SELECT pivot_a FROM PIVOT((SELECT value AS entity,'a' AS category,value AS amount FROM 'unopened.vortex'), '{options}') AS p"
    );
    for sql in [
        format!("SELECT ({pivot}) AS scalar FROM range(1,1)"),
        format!("SELECT CASE WHEN TRUE THEN 7 ELSE ({pivot}) END AS scalar"),
        format!("SELECT COALESCE((SELECT 7),({pivot})) AS scalar"),
        format!("SELECT value FROM range(1,1) WHERE value=({pivot})"),
        format!("SELECT SUM(({pivot})) AS scalar FROM range(1,1)"),
        format!("SELECT FIRST_VALUE(({pivot})) OVER (ORDER BY value) AS scalar FROM range(1,1)"),
        format!(
            "SELECT value FROM range(1,1) WHERE EXISTS (SELECT ({pivot}) AS scalar FROM range(1,2))"
        ),
        format!("SELECT * FROM (SELECT ({pivot}) AS scalar FROM range(1,1)) AS d"),
        format!("SELECT (SELECT 7) AS scalar UNION ALL SELECT ({pivot}) AS scalar"),
    ] {
        let error = prepare_with_inputs(
            &sql,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |_| panic!("schema rejection must precede input preparation: {sql}"),
            |_| panic!("schema rejection must precede source resolution: {sql}"),
        )
        .err()
        .expect("dynamic scalar schema must be rejected during preparation");
        assert!(
            error.to_string().contains("statically bound output schema"),
            "{sql}: {error}"
        );
    }
}

#[test]
fn native_scalar_subqueries_use_fresh_correlated_parameters_in_all_expression_sites() {
    for (sql, expected) in [
        (
            "SELECT value,(SELECT MAX(value) AS best FROM range(1,5) WHERE value<=outer.value) AS best FROM range(0,4)",
            json!([{"value":0,"best":null},{"value":1,"best":1},{"value":2,"best":2},{"value":3,"best":3}]),
        ),
        (
            "SELECT column_1,(SELECT outer.column_1 + 10 AS selected FROM range(1,2)) AS scalar FROM (VALUES (2),(2),(3),(NULL)) AS v",
            json!([{"column_1":2,"scalar":12},{"column_1":2,"scalar":12},{"column_1":3,"scalar":13},{"column_1":null,"scalar":null}]),
        ),
        (
            "SELECT SUM((SELECT outer.value AS selected FROM range(1,2))) AS total FROM range(1,4)",
            json!([{"total":6.0}]),
        ),
        (
            "SELECT LAST_VALUE((SELECT outer.value AS selected FROM range(1,2))) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last FROM range(1,4)",
            json!([{"last":3},{"last":3},{"last":3}]),
        ),
        (
            "SELECT CASE WHEN value=1 THEN (SELECT value FROM range(1,4) WHERE value<=outer.value) ELSE 9 END AS scalar FROM range(1,4)",
            json!([{"scalar":1},{"scalar":9},{"scalar":9}]),
        ),
        (
            "SELECT value FROM range(1,4) WHERE value=(SELECT outer.value AS selected FROM range(1,2))",
            json!([{"value":1},{"value":2},{"value":3}]),
        ),
        (
            "SELECT (SELECT value FROM range(1,5) WHERE value = outer.value) AS scalar FROM range(0,4)",
            json!([{"scalar":null},{"scalar":1},{"scalar":2},{"scalar":3}]),
        ),
        (
            "SELECT (SELECT (SELECT outer.value AS captured) AS inner_value FROM range(5,6)) AS scalar FROM range(1,3)",
            json!([{"scalar":5},{"scalar":5}]),
        ),
    ] {
        verify_memory(sql, &expected);
    }
}
