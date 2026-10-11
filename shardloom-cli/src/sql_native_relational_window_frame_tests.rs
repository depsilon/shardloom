use super::{memory_tests::verify_memory, *};

#[test]
fn native_relational_sql_window_frames_default_peers_and_unordered_partition() {
    verify_memory(
        "SELECT column_1,COUNT(*) OVER (ORDER BY column_1) AS n,SUM(column_2) OVER (ORDER BY column_1) AS total,COUNT(DISTINCT column_2) OVER () AS distinct_values,FIRST_VALUE(column_2) OVER () AS first,LAST_VALUE(column_2) OVER () AS last,NTH_VALUE(column_2,2) OVER () AS second FROM (VALUES (2,20),(1,NULL),(1,10),(3,20)) AS v",
        &json!([
            {"column_1":2,"n":3,"total":30.0,"distinct_values":2,"first":20,"last":20,"second":null},
            {"column_1":1,"n":2,"total":10.0,"distinct_values":2,"first":20,"last":20,"second":null},
            {"column_1":1,"n":2,"total":10.0,"distinct_values":2,"first":20,"last":20,"second":null},
            {"column_1":3,"n":4,"total":50.0,"distinct_values":2,"first":20,"last":20,"second":null},
        ]),
    );
    verify_memory(
        "SELECT COUNT(*) OVER () AS n,AVG(column_1) OVER () AS mean,MIN(column_1) OVER () AS smallest,MAX(column_1) OVER () AS largest,NTH_VALUE(column_1,18446744073709551615) OVER () AS absent FROM (VALUES (1),(NULL),(3)) AS v",
        &json!([
            {"n":3,"mean":2.0,"smallest":1,"largest":3,"absent":null},
            {"n":3,"mean":2.0,"smallest":1,"largest":3,"absent":null},
            {"n":3,"mean":2.0,"smallest":1,"largest":3,"absent":null},
        ]),
    );
}

#[test]
fn native_relational_sql_window_frames_computed_arguments_and_composition() {
    verify_memory(
        "SELECT value,SUM(value*2+1) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS total,COUNT(NULLIF(value,2)) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n,FIRST_VALUE(CASE WHEN value=2 THEN NULL ELSE value*10 END) OVER (ORDER BY value ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS chosen FROM range(1,5)",
        &json!([
            {"value":1,"total":5.0,"n":1,"chosen":10},
            {"value":2,"total":10.0,"n":1,"chosen":null},
            {"value":3,"total":14.0,"n":2,"chosen":30},
            {"value":4,"total":7.0,"n":3,"chosen":40},
        ]),
    );
    verify_memory(
        "SELECT SUM(total) AS all_totals FROM (SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM range(1,5)) AS framed",
        &json!([{"all_totals":16.0}]),
    );
    verify_memory(
        "SELECT COUNT(*) OVER (ORDER BY rows GROUPS CURRENT ROW EXCLUDE TIES) AS n FROM (SELECT value AS rows FROM range(1,4)) AS v",
        &json!([{"n":1},{"n":1},{"n":1}]),
    );
}

#[test]
fn native_relational_sql_window_frames_groups_range_and_ignored_ranking_frame() {
    verify_memory(
        "SELECT column_1,COUNT(*) OVER (ORDER BY column_1 GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS prior_group,COUNT(*) OVER (ORDER BY column_1 RANGE BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE TIES) AS nearby,ROW_NUMBER() OVER (ORDER BY column_1 ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS position FROM (VALUES (3),(1),(1),(7)) AS v",
        &json!([
            {"column_1":3,"prior_group":2,"nearby":3,"position":3},
            {"column_1":1,"prior_group":0,"nearby":1,"position":1},
            {"column_1":1,"prior_group":0,"nearby":1,"position":2},
            {"column_1":7,"prior_group":1,"nearby":1,"position":4},
        ]),
    );
    verify_memory(
        "SELECT value,COUNT(*) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) AS n,SUM(value) OVER (ORDER BY value ROWS BETWEEN 18446744073709551615 FOLLOWING AND UNBOUNDED FOLLOWING) AS missing FROM range(1,4)",
        &json!([{"value":1,"n":0,"missing":null},{"value":2,"n":0,"missing":null},{"value":3,"n":0,"missing":null}]),
    );
}

#[test]
fn native_relational_sql_window_frames_temporal_and_exact_decimal_range() {
    verify_memory(
        "SELECT COUNT(*) OVER (ORDER BY column_1 RANGE BETWEEN .5 PRECEDING AND CURRENT ROW) AS n FROM (VALUES (1.0),(1.25),(2.0)) AS values_with_fractional_gap",
        &json!([{"n":1},{"n":2},{"n":1}]),
    );
    verify_memory(
        "SELECT COUNT(*) OVER (ORDER BY day RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS n FROM (SELECT CAST(column_1 AS date32) AS day FROM (VALUES ('2025-01-01'),('2025-01-02'),('2025-01-04')) AS text_days) AS days",
        &json!([{"n":1},{"n":2},{"n":1}]),
    );
    verify_memory(
        "SELECT COUNT(*) OVER (ORDER BY time RANGE BETWEEN INTERVAL '1500' MILLISECOND PRECEDING AND CURRENT ROW) AS n FROM (SELECT CAST(column_1 AS timestamp_micros) AS time FROM (VALUES ('2025-01-01T00:00:00Z'),('2025-01-01T00:00:01Z'),('2025-01-01T00:00:03Z')) AS times) AS typed_times",
        &json!([{"n":1},{"n":2},{"n":1}]),
    );
    verify_memory(
        "SELECT COUNT(*) OVER (ORDER BY money RANGE BETWEEN CAST('0.10' AS decimal128(4,2)) PRECEDING AND CURRENT ROW) AS n FROM (SELECT CAST(column_1 AS decimal128(6,3)) AS money FROM (VALUES ('1.000'),('1.099'),('1.101')) AS text_money) AS amounts",
        &json!([{"n":1},{"n":2},{"n":2}]),
    );
}

#[test]
fn native_relational_sql_window_frames_reject_invalid_empty_declarations() {
    for projection in [
        "COUNT(*) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS n",
        "COUNT(*) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) AS n",
        "COUNT(*) OVER (ROWS -1 PRECEDING) AS n",
        "COUNT(*) OVER (ROWS 1.5 PRECEDING) AS n",
        "COUNT(*) OVER (GROUPS CURRENT ROW) AS n",
        "COUNT(*) OVER (RANGE 1 PRECEDING) AS n",
        "COUNT(*) OVER (ORDER BY value,value RANGE 1 PRECEDING) AS n",
        "COUNT(*) OVER (ORDER BY value RANGE INTERVAL '1' DAY PRECEDING) AS n",
        "COUNT(*) OVER (ORDER BY value ROWS INTERVAL '1' DAY PRECEDING) AS n",
        "COUNT(*) OVER (ORDER BY value RANGE INTERVAL '1' MONTH PRECEDING) AS n",
        "COUNT(*) OVER (ORDER BY value ROWS CURRENT ROW EXCLUDE UNKNOWN) AS n",
        "COUNT(*) OVER (ORDER BY value ROWS BETWEEN CURRENT ROW) AS n",
        "NTH_VALUE(value,0) OVER () AS n",
        "NTH_VALUE(value,1.5) OVER () AS n",
        "FIRST_VALUE(absent) OVER () AS n",
        "SUM(absent+1) OVER () AS n",
        "ROW_NUMBER() OVER (ORDER BY value ROWS -1 PRECEDING) AS n",
    ] {
        let statement = format!("SELECT {projection} FROM range(0,0)");
        assert!(
            prepare(
                &statement,
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
                    .expect("explicit fixture allocation"),
                |_| { panic!("empty analytic declaration must not resolve a file") }
            )
            .is_err(),
            "{statement}",
        );
    }
    verify_memory(
        "SELECT SUM(value+1) OVER () AS total FROM range(0,0)",
        &json!([]),
    );
}
