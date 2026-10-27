use super::{memory_tests::verify_memory, *};

#[test]
fn native_relational_sql_aggregate_filter_preserves_groups_and_independent_domains() {
    verify_memory(
        "SELECT column_1,COUNT(*) AS all_rows,COUNT(*) FILTER (WHERE column_3) AS admitted,SUM(column_2) FILTER (WHERE column_3) AS total,VAR_POP(column_2) FILTER (WHERE column_3) AS variance,VAR_POP(column_2) FILTER (WHERE NOT column_3) AS other_variance,MEDIAN(column_2) FILTER (WHERE column_3) AS median,MEDIAN(column_2) FILTER (WHERE NOT column_3) AS other_median,ARRAY_AGG(column_2 ORDER BY column_2 DESC NULLS LAST) FILTER (WHERE column_3) AS items FROM (VALUES ('a',1,true),('a',3,true),('a',5,false),('a',9,false),('a',99,NULL),('b',7,false),('c',NULL,true)) AS v GROUP BY column_1 ORDER BY column_1",
        &json!([
            {"column_1":"a","all_rows":5,"admitted":2,"total":4.0,"variance":1.0,"other_variance":4.0,"median":2.0,"other_median":7.0,"items":[3,1]},
            {"column_1":"b","all_rows":1,"admitted":0,"total":null,"variance":null,"other_variance":0.0,"median":null,"other_median":7.0,"items":null},
            {"column_1":"c","all_rows":1,"admitted":1,"total":null,"variance":null,"other_variance":null,"median":null,"other_median":null,"items":[null]}
        ]),
    );
    verify_memory(
        "SELECT SUM(DISTINCT column_1) FILTER (WHERE column_2) AS total,STRING_AGG(column_3, '/' ORDER BY column_1 DESC) FILTER (WHERE column_2) AS text FROM (VALUES (1,true,'a'),(1,true,'b'),(3,false,'c'),(5,true,'d')) AS v HAVING SUM(DISTINCT column_1) FILTER (WHERE column_2)>5 ORDER BY SUM(column_1) FILTER (WHERE NOT column_2)",
        &json!([{"total":6.0,"text":"d/a/b"}]),
    );
}

#[test]
fn native_relational_sql_aggregate_filter_guards_computed_and_scoped_arguments() {
    verify_memory(
        "SELECT COUNT(*) FILTER (WHERE NULL) AS n,SUM(value) FILTER (WHERE NULL) AS total FROM range(0,3)",
        &json!([{"n":0,"total":null}]),
    );
    verify_memory(
        "SELECT value,COUNT(*) FILTER (WHERE NULL) OVER (ORDER BY value) AS n FROM range(0,2)",
        &json!([{"value":0,"n":0},{"value":1,"n":0}]),
    );
    verify_memory(
        "WITH input AS (SELECT column_1 AS value,column_2 AS keep FROM (VALUES ('2',true),('bad',false),('bad',NULL)) AS v) SELECT SUM(CAST(value AS INT64)) FILTER (WHERE keep) AS total,COUNT(CAST(value AS INT64)) FILTER (WHERE keep) AS n,LIST(CAST(value AS INT64)) FILTER (WHERE keep) AS items FROM input",
        &json!([{"total":2.0,"n":1,"items":[2]}]),
    );
    verify_memory(
        "WITH input AS (SELECT column_1 AS value,column_2 AS keep FROM (VALUES (2,true),(3,false)) AS v), allowed AS (SELECT 2 AS value) SELECT SUM(value) FILTER (WHERE value IN (SELECT value FROM allowed)) AS total FROM input",
        &json!([{"total":2.0}]),
    );
    verify_memory(
        "SELECT COUNT(*) FILTER (WHERE false) AS n,LIST(value) FILTER (WHERE false) AS items FROM range(0,0)",
        &json!([{"n":0,"items":null}]),
    );
}

#[test]
fn native_relational_sql_aggregate_filter_orders_by_hidden_measures() {
    verify_memory(
        "SELECT column_1,COUNT(*) AS n FROM (VALUES ('a',1,true),('a',8,false),('b',7,true),('b',2,false)) AS v GROUP BY column_1 HAVING SUM(column_2) FILTER (WHERE column_3)>0 ORDER BY SUM(column_2) FILTER (WHERE column_3) DESC, SUM(column_2) FILTER (WHERE NOT column_3)",
        &json!([{"column_1":"b","n":2},{"column_1":"a","n":2}]),
    );
    verify_memory(
        "SELECT column_1 FROM (VALUES ('a',1),('b',3),('a',4)) AS v GROUP BY column_1 ORDER BY SUM(column_2) DESC",
        &json!([{"column_1":"a"},{"column_1":"b"}]),
    );
}

#[test]
fn native_relational_sql_aggregate_filter_windows_preserve_frame_positions_and_nulls() {
    verify_memory(
        "SELECT column_1,COUNT(*) FILTER (WHERE column_3) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n,SUM(CAST(column_2 AS INT64)) FILTER (WHERE column_3) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS total,FIRST(column_2 ORDER BY column_1 DESC) FILTER (WHERE column_3) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS first,LIST(column_2) FILTER (WHERE column_3) OVER (ORDER BY column_1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS items FROM (VALUES (0,'1',true),(1,'bad',false),(2,'3',true),(3,'bad',NULL),(4,NULL,true)) AS v ORDER BY column_1",
        &json!([
            {"column_1":0,"n":1,"total":1.0,"first":"1","items":["1"]},
            {"column_1":1,"n":2,"total":4.0,"first":"3","items":["1","3"]},
            {"column_1":2,"n":1,"total":3.0,"first":"3","items":["3"]},
            {"column_1":3,"n":2,"total":3.0,"first":null,"items":["3",null]},
            {"column_1":4,"n":1,"total":null,"first":null,"items":[null]}
        ]),
    );
}
