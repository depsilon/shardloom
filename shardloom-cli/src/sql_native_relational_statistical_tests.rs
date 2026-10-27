use super::{memory_tests::verify_memory, *};

#[test]
fn native_relational_sql_ordered_selectors_preserve_values_ties_and_hidden_order_dependencies() {
    verify_memory(
        "SELECT column_1,FIRST(column_2) AS initial,LAST(column_2 ORDER BY column_3 NULLS LAST) AS ending,ARG_MIN(column_2,column_3 ORDER BY column_4 DESC NULLS LAST) AS least,ARG_MAX(column_2,column_3 ORDER BY column_4 DESC NULLS LAST) AS greatest FROM (VALUES ('a',NULL,0,0),('a','x',2,1),('a','y',2,3),('a','z',1,NULL),('b',NULL,NULL,2)) AS v GROUP BY column_1 ORDER BY column_1",
        &json!([{"column_1":"a","initial":null,"ending":"y","least":"z","greatest":"y"},
            {"column_1":"b","initial":null,"ending":null,"least":null,"greatest":null}]),
    );
    verify_memory(
        "SELECT ARG_MIN(column_1+1,-column_2 ORDER BY column_3) AS chosen,FIRST(column_1+1 ORDER BY column_3 DESC) AS reverse FROM (VALUES (1,10,3),(4,20,2),(7,20,1)) AS v HAVING ARG_MIN(column_1+1,-column_2 ORDER BY column_3)<>ARG_MIN(column_1+1,-column_2 ORDER BY column_3 DESC) ORDER BY FIRST(column_1+1 ORDER BY column_3 DESC)",
        &json!([{"chosen":8,"reverse":2}]),
    );
    verify_memory(
        "SELECT FIRST(v.column_1 ORDER BY v.column_2),FIRST(v.column_1 ORDER BY v.column_2 DESC) FROM (VALUES (1,2),(3,1)) AS v",
        &{
            let ascending = parse_aggregate_projection("FIRST(v.column_1 ORDER BY v.column_2)")
                .unwrap()
                .unwrap()
                .output_name();
            let descending =
                parse_aggregate_projection("FIRST(v.column_1 ORDER BY v.column_2 DESC)")
                    .unwrap()
                    .unwrap()
                    .output_name();
            json!([{ascending:3,descending:1}])
        },
    );
}

#[test]
fn native_relational_sql_ordered_selector_windows_keep_local_order_separate_from_frame_order() {
    verify_memory(
        "SELECT column_1,FIRST(column_2 ORDER BY column_3 DESC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS initial,LAST(column_2 ORDER BY column_3 DESC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ending,ARG_MIN(column_2,column_3 ORDER BY column_1 DESC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS least,ARG_MAX(column_2,column_3 ORDER BY column_1 DESC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS greatest FROM (VALUES (0,'a',2),(1,NULL,1),(2,'c',2),(3,'d',3)) AS v ORDER BY column_1",
        &json!([{"column_1":0,"initial":"a","ending":null,"least":"a","greatest":"a"},
            {"column_1":1,"initial":"a","ending":null,"least":"c","greatest":"c"},
            {"column_1":2,"initial":"d","ending":null,"least":"c","greatest":"d"},
            {"column_1":3,"initial":"d","ending":"c","least":"c","greatest":"d"}]),
    );
    verify_memory(
        "SELECT FIRST(value) OVER (ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS empty FROM range(1,4)",
        &json!([{"empty":null},{"empty":null},{"empty":null}]),
    );
}

#[test]
fn native_relational_sql_ordered_collection_aggregates_preserve_nulls_and_separators() {
    verify_memory(
        "SELECT column_1,LIST(column_2) AS original,ARRAY_AGG(column_2 ORDER BY column_3 ASC NULLS LAST) AS ordered,STRING_AGG(column_2, '/' ORDER BY column_3 ASC NULLS LAST) AS joined,STRING_AGG(column_2, '''東京,' ORDER BY column_3 ASC NULLS LAST) AS quoted FROM (VALUES ('a',NULL,3),('a','東京',2),('a','',1),('b',NULL,1),('b',NULL,2)) AS v GROUP BY column_1 ORDER BY column_1",
        &json!([
            {"column_1":"a","original":[null,"東京", ""],"ordered":["","東京",null],"joined":"/東京","quoted":"'東京,東京"},
            {"column_1":"b","original":[null,null],"ordered":[null,null],"joined":null,"quoted":null}
        ]),
    );
    verify_memory(
        "SELECT column_1,ARRAY_AGG(column_2 ORDER BY column_3 DESC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS items,STRING_AGG(column_2, '/' ORDER BY column_3 ASC) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS joined FROM (VALUES (0,'a',2),(1,NULL,1),(2,'c',2),(3,'d',3)) AS v ORDER BY column_1",
        &json!([
            {"column_1":0,"items":["a",null],"joined":"a"},
            {"column_1":1,"items":["a","c",null],"joined":"a/c"},
            {"column_1":2,"items":["d","c",null],"joined":"c/d"},
            {"column_1":3,"items":["d","c"],"joined":"c/d"}
        ]),
    );
    verify_memory(
        "SELECT LIST(value) OVER (ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS empty FROM range(1,4)",
        &json!([{"empty":null},{"empty":null},{"empty":null}]),
    );
}

#[test]
fn native_relational_sql_exact_quantile_levels_computed_having_and_windows_are_independent() {
    verify_memory(
        "SELECT MEDIAN(column_1+1) AS center,QUANTILE_CONT(column_1,0.25) AS low,QUANTILE_CONT(column_1,0.75) AS high,QUANTILE_DISC(column_1,0.5) AS selected FROM (VALUES (1),(1),(5),(9),(NULL)) AS v HAVING QUANTILE_CONT(column_1,0.25)<QUANTILE_CONT(column_1,0.75) ORDER BY QUANTILE_CONT(column_1,0.25)",
        &json!([{"center":4.0,"low":1.0,"high":6.0,"selected":1}]),
    );
    verify_memory(
        "SELECT value,MEDIAN(value+1) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS center,QUANTILE_CONT(value,0.25) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS low,QUANTILE_DISC(value,0.5) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS selected FROM range(1,5)",
        &json!([{"value":1,"center":2.5,"low":1.25,"selected":1},
            {"value":2,"center":3.0,"low":1.5,"selected":2},
            {"value":3,"center":4.0,"low":2.5,"selected":3},
            {"value":4,"center":4.5,"low":3.25,"selected":3}]),
    );
    verify_memory(
        "SELECT QUANTILE_DISC(value,1.0) OVER (ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS empty FROM range(1,4)",
        &json!([{"empty":null},{"empty":null},{"empty":null}]),
    );
}

#[test]
fn native_relational_sql_distinct_additive_computed_having_and_window_values() {
    verify_memory(
        "SELECT SUM(DISTINCT column_1+1) AS total,AVG(DISTINCT column_1+1) AS average FROM (VALUES (1),(1),(3),(NULL)) AS v HAVING SUM(DISTINCT column_1+1)>5",
        &json!([{"total":6.0,"average":3.0}]),
    );
    verify_memory(
        "SELECT column_1,SUM(DISTINCT column_2) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS total,AVG(DISTINCT column_2) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS average FROM (VALUES (0,1),(1,1),(2,3),(3,NULL)) AS v",
        &json!([{"column_1":0,"total":1.0,"average":1.0}, {"column_1":1,"total":4.0,"average":2.0},
                {"column_1":2,"total":4.0,"average":2.0}, {"column_1":3,"total":3.0,"average":3.0}]),
    );
}

#[test]
fn native_relational_sql_statistical_aliases_computed_arguments_and_having_share_execution() {
    verify_memory(
        "WITH input AS (SELECT column_1 AS x,column_2 AS y FROM (VALUES (1,2),(2,1),(3,4),(4,3),(NULL,99),(99,NULL)) AS v) SELECT COVAR_POP(x,y) AS population,COVAR_SAMP(x,y) AS sample FROM input HAVING COVAR_POP(x, y) > 0",
        &json!([{"population":0.75,"sample":1.0}]),
    );
    verify_memory(
        "SELECT VAR_POP(COALESCE(column_1,2)) AS population,VAR_SAMP(COALESCE(column_1,2)) AS sample,STDDEV_POP(column_1) AS deviation FROM (VALUES (2),(4),(4),(4),(5),(5),(7),(9)) AS v",
        &json!([{"population":4.0,"sample":32.0/7.0,"deviation":2.0}]),
    );
    verify_memory(
        "SELECT column_1,VARIANCE(column_2) AS variance,STD(column_2) AS deviation,CORR(column_2,column_2) AS correlation FROM (VALUES ('empty',NULL),('single',5),('constant',7),('constant',7)) AS v GROUP BY column_1 ORDER BY column_1",
        &json!([
            {"column_1":"constant","variance":0.0,"deviation":0.0,"correlation":null},
            {"column_1":"empty","variance":null,"deviation":null,"correlation":null},
            {"column_1":"single","variance":null,"deviation":null,"correlation":null},
        ]),
    );
    verify_memory(
        "SELECT COVAR_POP(value,(SELECT 2 AS multiplier)*value) AS covariance FROM range(1,5)",
        &json!([{"covariance":2.5}]),
    );
}

#[test]
fn native_relational_sql_statistical_windows_keep_both_computed_inputs_and_empty_frames() {
    verify_memory(
        "SELECT value,VAR_POP(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS variance,COVAR_POP(value,value*2) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS covariance FROM range(1,5)",
        &json!([
            {"value":1,"variance":0.0,"covariance":0.0},
            {"value":2,"variance":0.25,"covariance":0.5},
            {"value":3,"variance":0.25,"covariance":0.5},
            {"value":4,"variance":0.25,"covariance":0.5},
        ]),
    );
    verify_memory(
        "SELECT STDDEV(value) OVER (ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS empty FROM range(1,4)",
        &json!([{"empty":null},{"empty":null},{"empty":null}]),
    );
}
