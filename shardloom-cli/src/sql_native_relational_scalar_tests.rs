use super::memory_tests::verify_memory;
use super::*;

#[test]
fn native_minute_sql_forms_preserve_epoch_boundaries_and_nulls() {
    verify_memory(
        "SELECT column_1 AS t,EXTRACT(minute FROM column_1) AS m,DATE_TRUNC('minute',column_1) AS bucket FROM (VALUES (-61),(-60),(-1),(0),(59),(60),(61),(3599),(3600),(NULL)) AS v ORDER BY t NULLS LAST",
        &json!([
            {"t":-61,"m":58,"bucket":-120},
            {"t":-60,"m":59,"bucket":-60},
            {"t":-1,"m":59,"bucket":-60},
            {"t":0,"m":0,"bucket":0},
            {"t":59,"m":0,"bucket":0},
            {"t":60,"m":1,"bucket":60},
            {"t":61,"m":1,"bucket":60},
            {"t":3599,"m":59,"bucket":3540},
            {"t":3600,"m":0,"bucket":3600},
            {"t":null,"m":null,"bucket":null},
        ]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM NULL) AS m,DATE_TRUNC('minute',NULL) AS bucket",
        &json!([{"m":null,"bucket":null}]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM value) AS m,DATE_TRUNC('minute',value) AS bucket FROM range(0,0)",
        &json!([]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM CAST(3599 AS uint64)) AS m,DATE_TRUNC('minute',CAST(3599 AS uint64)) AS bucket",
        &json!([{"m":59,"bucket":3540}]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM '2024-02-29 12:34:56') AS m,DATE_TRUNC('minute','2024-02-29 12:34:56') AS bucket",
        &json!([{"m":34,"bucket":"2024-02-29 12:34:00"}]),
    );
}

#[test]
fn native_minute_sql_forms_compose_in_predicates_aggregates_and_lazy_scalars() {
    verify_memory(
        "SELECT column_1 AS t,EXTRACT(minute FROM COALESCE(column_1,59)+1) AS m FROM (VALUES (59),(60),(119),(NULL)) AS v WHERE EXTRACT(minute FROM COALESCE(column_1,59)+1)=1 ORDER BY t NULLS LAST",
        &json!([{"t":59,"m":1},{"t":60,"m":1},{"t":null,"m":1}]),
    );
    verify_memory(
        "SELECT SUM(EXTRACT(minute FROM value*60)) AS total FROM range(0,3)",
        &json!([{"total":3.0}]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM (SELECT 120)) AS m,DATE_TRUNC('minute',CASE WHEN TRUE THEN 61 ELSE 1/0 END) AS bucket",
        &json!([{"m":2,"bucket":60}]),
    );
}

#[test]
fn native_minute_sql_forms_reuse_selected_group_and_order_expressions() {
    verify_memory(
        "SELECT DATE_TRUNC('minute',column_1) AS m,COUNT(*) AS n FROM (VALUES (121),(60),(59),(61),(120),(NULL)) AS v GROUP BY date_trunc('MINUTE', column_1) ORDER BY DATE_TRUNC('minute',column_1) DESC NULLS LAST LIMIT 2 OFFSET 1",
        &json!([{"m":60,"n":2},{"m":0,"n":1}]),
    );
    verify_memory(
        "SELECT EXTRACT(minute FROM column_1+1) AS m,COUNT(*) AS n FROM (VALUES (59),(60),(119),(NULL)) AS v GROUP BY m ORDER BY EXTRACT(MINUTE FROM column_1 + 1) NULLS FIRST",
        &json!([{"m":null,"n":1},{"m":1,"n":2},{"m":2,"n":1}]),
    );
}

#[test]
fn native_minute_sql_forms_keep_typed_timestamp_units_explicit() {
    verify_memory(
        "SELECT TIMESTAMP_MINUTE(CAST('1970-01-01T00:01:00Z' AS timestamp_micros)) AS m",
        &json!([{"m":1}]),
    );
    for expression in [
        "EXTRACT(minute FROM CAST('1970-01-01T00:01:00Z' AS timestamp_micros))",
        "DATE_TRUNC('minute',CAST('1970-01-01T00:01:00Z' AS timestamp_micros))",
    ] {
        let error = prepare(
            &format!("SELECT {expression} AS m"),
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
            |_| panic!("memory only"),
        )
        .err()
        .expect("epoch-second helpers must not reinterpret typed microseconds");
        assert!(
            error
                .to_string()
                .contains("minute functions require integer epoch seconds or timestamp text"),
            "{error}"
        );
    }
}

#[test]
fn native_relational_predicate_values_preserve_selected_case_and_coalesce_demand() {
    for (sql, expected) in [
        (
            "SELECT value,CASE WHEN value=1 THEN EXISTS (SELECT value FROM range(1,3) WHERE value=outer.value) ELSE FALSE END AS answer FROM range(1,3)",
            json!([{"value":1,"answer":true},{"value":2,"answer":false}]),
        ),
        (
            "SELECT CASE WHEN FALSE THEN EXISTS (SELECT 1/0 AS bad FROM range(1,2)) ELSE TRUE END AS answer",
            json!([{"answer":true}]),
        ),
        (
            "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN NOT EXISTS (SELECT 1/0 AS bad FROM range(1,2)) ELSE TRUE END AS answer",
            json!([{"answer":true}]),
        ),
        (
            "SELECT CASE WHEN FALSE THEN value IN (SELECT 1/0 AS bad FROM range(1,2)) ELSE TRUE END AS answer FROM range(1,2)",
            json!([{"answer":true}]),
        ),
        (
            "SELECT COALESCE(TRUE,EXISTS (SELECT 1/0 AS bad FROM range(1,2))) AS answer",
            json!([{"answer":true}]),
        ),
        (
            "SELECT COALESCE(CAST(NULL AS BOOLEAN),NOT EXISTS (SELECT value FROM range(1,1))) AS answer",
            json!([{"answer":true}]),
        ),
        (
            "SELECT CASE WHEN TRUE THEN value > ANY (SELECT value FROM range(1,2)) ELSE FALSE END AS answer FROM range(1,3)",
            json!([{"answer":false},{"answer":true}]),
        ),
        (
            "SELECT CASE WHEN TRUE THEN value <= ALL (SELECT value FROM range(1,2)) ELSE FALSE END AS answer FROM range(1,3)",
            json!([{"answer":true},{"answer":false}]),
        ),
        (
            "SELECT CASE WHEN TRUE THEN (value,other) IN (SELECT value,value+1 AS other FROM range(1,2)) ELSE FALSE END AS answer FROM (SELECT value,value+1 AS other FROM range(1,3)) AS outer_values",
            json!([{"answer":true},{"answer":false}]),
        ),
    ] {
        verify_memory(sql, &expected);
    }
    for expression in [
        "EXISTS (SELECT 1/0 AS bad FROM range(1,2))",
        "value IN (SELECT 1/0 AS bad FROM range(1,2))",
        "value > ANY (SELECT 1/0 AS bad FROM range(1,2))",
    ] {
        let sql = format!(
            "SELECT CASE WHEN TRUE THEN {expression} ELSE FALSE END AS answer FROM range(1,2)"
        );
        let prepared = prepare(
            &sql,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
            |_| panic!("memory only"),
        )
        .unwrap();
        let error = prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .expect("selected predicate must evaluate its inner expression");
        assert!(
            error.to_string().contains("division by zero"),
            "{sql}: {error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

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
            "SELECT CAST((SELECT 7) AS int64) + (SELECT 3) AS scalar",
            json!([{"scalar":10}]),
        ),
        (
            "SELECT TRY_CAST((SELECT 7) AS int64) % (SELECT 3) AS scalar",
            json!([{"scalar":1}]),
        ),
        (
            "SELECT value FROM range(1,4) WHERE CAST((SELECT 7) AS int64) - value > 4",
            json!([{"value":1},{"value":2}]),
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
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
                .expect("explicit fixture allocation"),
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
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
                    .expect("explicit fixture allocation"),
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
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
                .expect("explicit fixture allocation"),
            |_| panic!("schema rejection must precede input preparation: {sql}"),
            |_, _| panic!("schema rejection must precede source resolution: {sql}"),
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
