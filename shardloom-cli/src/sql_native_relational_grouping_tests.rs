use super::{memory_tests::verify_memory, *};

#[test]
fn native_sql_grouping_sets_preserve_nulls_duplicates_filters_and_original_arguments() {
    verify_memory(
        "WITH v AS (SELECT column_1 AS g,column_2 AS r,column_3 AS value,column_4 AS keep FROM (VALUES ('A',1,2,true),('A',2,4,false),('B',1,6,true),(NULL,1,8,true)) AS x) SELECT g,r,GROUPING(g,r) AS mask,COUNT(*) AS n,SUM(r) AS sum_r,SUM(value) FILTER (WHERE keep) AS total FROM v GROUP BY GROUPING SETS ((g,r),(g),(),(g)) ORDER BY mask,g NULLS LAST,r NULLS LAST",
        &json!([
            {"g":"A","r":1,"mask":0,"n":1,"sum_r":1.0,"total":2.0},
            {"g":"A","r":2,"mask":0,"n":1,"sum_r":2.0,"total":null},
            {"g":"B","r":1,"mask":0,"n":1,"sum_r":1.0,"total":6.0},
            {"g":null,"r":1,"mask":0,"n":1,"sum_r":1.0,"total":8.0},
            {"g":"A","r":null,"mask":1,"n":2,"sum_r":3.0,"total":2.0},
            {"g":"A","r":null,"mask":1,"n":2,"sum_r":3.0,"total":2.0},
            {"g":"B","r":null,"mask":1,"n":1,"sum_r":1.0,"total":6.0},
            {"g":"B","r":null,"mask":1,"n":1,"sum_r":1.0,"total":6.0},
            {"g":null,"r":null,"mask":1,"n":1,"sum_r":1.0,"total":8.0},
            {"g":null,"r":null,"mask":1,"n":1,"sum_r":1.0,"total":8.0},
            {"g":null,"r":null,"mask":3,"n":4,"sum_r":5.0,"total":16.0}
        ]),
    );
}

#[test]
fn native_sql_grouping_sets_rollup_cube_atomic_units_and_cartesian_products() {
    for (grouping, masks) in [
        ("CUBE((g,r),c)", vec![0, 1, 6, 7]),
        ("ROLLUP((g,r),c)", vec![0, 1, 7]),
        ("g,ROLLUP(r),CUBE(c)", vec![0, 1, 2, 3]),
        ("GROUPING SETS (ROLLUP((g,r),c),())", vec![0, 1, 7, 7]),
    ] {
        let expected = masks
            .into_iter()
            .map(|mask| json!({"mask":mask,"n":1}))
            .collect::<Vec<_>>();
        verify_memory(
            &format!(
                "WITH v AS (SELECT 'A' AS g,2 AS r,3 AS c) SELECT GROUPING_ID(g,r,c) AS mask,COUNT(*) AS n FROM v GROUP BY {grouping} ORDER BY mask"
            ),
            &json!(expected),
        );
    }
    for (modifier, count) in [("", 4), ("ALL ", 4), ("DISTINCT ", 2)] {
        verify_memory(
            &format!(
                "SELECT COUNT(*) AS n FROM (SELECT value,COUNT(*) AS n FROM range(0,1) GROUP BY {modifier}ROLLUP(value),ROLLUP(value)) AS x"
            ),
            &json!([{"n":count}]),
        );
    }
}

#[test]
fn native_sql_grouping_sets_empty_input_and_grouping_without_measures() {
    verify_memory(
        "SELECT COUNT(*) AS n,LIST(value) AS items FROM range(0,0) GROUP BY ()",
        &json!([{"n":0,"items":null}]),
    );
    verify_memory(
        "SELECT 7 AS value FROM range(0,0) GROUP BY GROUPING SETS ((),())",
        &json!([{"value":7},{"value":7}]),
    );
    verify_memory(
        "SELECT value,GROUPING(value) AS mask,COUNT(*) AS n FROM range(0,0) GROUP BY GROUPING SETS ((value),(),()) ORDER BY mask",
        &json!([
        {"value":null,"mask":1,"n":0},{"value":null,"mask":1,"n":0}]),
    );
    verify_memory(
        "SELECT value,GROUPING(value) FROM range(0,2) GROUP BY value ORDER BY value",
        &json!([
        {"value":0,"grouping":0},{"value":1,"grouping":0}]),
    );
    verify_memory(
        "SELECT value FROM range(0,2) GROUP BY GROUPING SETS ((value),()) ORDER BY value NULLS LAST",
        &json!([
        {"value":0},{"value":1},{"value":null}]),
    );
}

#[test]
fn native_sql_grouping_sets_composed_having_order_aliases_and_scopes() {
    verify_memory(
        "SELECT value+1 AS next,CAST(GROUPING(value+1) AS int64)+10 AS mask,COUNT(*) AS n FROM range(0,2) GROUP BY ROLLUP(1) HAVING GROUPING(next)=1 ORDER BY GROUPING(next)",
        &json!([{"next":null,"mask":11,"n":2}]),
    );
    verify_memory(
        "SELECT v.value AS g,COUNT(*) AS n FROM range(0,2) AS v GROUP BY ROLLUP(v.value) HAVING GROUPING(value)=1 ORDER BY GROUPING_ID(v.value)",
        &json!([{"g":null,"n":2}]),
    );
    verify_memory(
        "SELECT value FROM range(0,3) WHERE value IN (SELECT GROUPING(value) AS g FROM range(0,2) GROUP BY ROLLUP(value)) ORDER BY value",
        &json!([{"value":0},{"value":1}]),
    );
    verify_memory(
        "SELECT GROUPING(value) AS mask,(SELECT MAX(g) AS max_g FROM (SELECT GROUPING(value) AS g FROM range(0,1) GROUP BY ROLLUP(value)) AS x) AS inner_mask FROM range(0,1) GROUP BY ROLLUP(value) ORDER BY mask",
        &json!([{"mask":0,"inner_mask":1},{"mask":1,"inner_mask":1}]),
    );
    verify_memory(
        "SELECT GROUPING(value) AS mask,COUNT(*) AS n FROM range(0,3) GROUP BY ROLLUP(value) ORDER BY GROUPING(value) DESC LIMIT 1",
        &json!([{"mask":1,"n":3}]),
    );
}

#[test]
fn native_sql_grouping_sets_metadata_denial_precedes_source_open_and_invalid_scopes_fail() {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let query = format!(
        "SELECT COUNT(*) AS n FROM 'missing.vortex' GROUP BY CUBE({})",
        vec!["g"; 25].join(",")
    );
    assert!(
        prepare(&query, policy, |_| panic!(
            "source opened before grouping metadata admission"
        ))
        .is_err()
    );
    for query in [
        "SELECT GROUPING(value) FROM range(0,1)",
        "SELECT GROUPING(missing) FROM range(0,1) GROUP BY value",
        "SELECT SUM(GROUPING(value)) AS n FROM range(0,1) GROUP BY value",
        "SELECT COUNT(*) FILTER (WHERE GROUPING(value)=0) AS n FROM range(0,1) GROUP BY value",
        "SELECT COUNT(*) AS n FROM range(0,1) GROUP BY GROUPING SETS ()",
        // Masks retain the full UInt64 domain. Mixed signed/unsigned arithmetic
        // needs the same explicit lossless cast as an ordinary UInt64 column.
        "SELECT GROUPING(value)+10 AS n FROM range(0,1) GROUP BY ROLLUP(value)",
    ] {
        let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap();
        assert!(
            prepare(query, policy, |_| panic!("unexpected source")).is_err(),
            "{query}"
        );
    }
}

#[test]
fn native_sql_grouping_sets_indicators_feed_post_aggregate_windows() {
    verify_memory(
        "SELECT value,GROUPING(value) AS mask,SUM(GROUPING(value)) OVER () AS total FROM range(0,3) GROUP BY ROLLUP(value) ORDER BY mask,value NULLS LAST",
        &json!([
            {"value":0,"mask":0,"total":1.0},
            {"value":1,"mask":0,"total":1.0},
            {"value":2,"mask":0,"total":1.0},
            {"value":null,"mask":1,"total":1.0}
        ]),
    );
}
