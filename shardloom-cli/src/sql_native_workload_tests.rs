use super::*;

#[test]
fn native_workload_sql_ctes_lower_to_shared_sources_joins_and_sets() {
    let source = fixture();
    verify(
        &format!(
            "WITH overlay AS (SELECT value AS id,metric FROM '{source}' WHERE value=1), \
         base_kept AS (SELECT f.value AS id,f.metric FROM '{source}' AS f LEFT JOIN overlay AS o ON f.value=o.id WHERE o.id IS NULL), \
         merged AS (SELECT id,metric FROM base_kept UNION ALL SELECT id,metric+100 AS metric FROM overlay) \
         SELECT COUNT(*) AS rows,SUM(metric) AS total FROM merged"
        ),
        &json!([{"rows":5,"total":250.0}]),
    );
    verify(
        &format!(
            "WITH named AS (SELECT value FROM '{source}') SELECT named.value,'FROM named WITH ''literal''' AS text FROM named WHERE value=2"
        ),
        &json!([{"named.value":2,"text":"FROM named WITH 'literal'"}]),
    );
    for query in [
        "WITH RECURSIVE x AS (SELECT * FROM x) SELECT * FROM x",
        "WITH x AS (SELECT * FROM x) SELECT * FROM x",
        "WITH x AS (SELECT * FROM y),y AS (SELECT 1 AS value) SELECT * FROM x",
        "WITH x AS (SELECT 1 AS value),X AS (SELECT 2 AS value) SELECT * FROM x",
    ] {
        assert!(source_count(query).is_err(), "{query}");
    }
}

#[test]
fn native_workload_sql_remainder_group_aliases_and_input_precedence() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value % 2 AS bucket,COUNT(*) AS rows,SUM(metric) AS total FROM '{source}' GROUP BY bucket ORDER BY bucket"
        ),
        &json!([{"bucket":0,"rows":2,"total":60.0},{"bucket":1,"rows":3,"total":90.0}]),
    );
    verify(
        &format!(
            "SELECT value+1 AS value,COUNT(*) AS rows FROM '{source}' GROUP BY value ORDER BY value"
        ),
        &json!([{"value":2,"rows":1},{"value":3,"rows":1},{"value":4,"rows":1},{"value":5,"rows":1},{"value":6,"rows":1}]),
    );
    verify(
        &format!(
            "SELECT -5 % 3 AS a,5 % -3 AS b,5.5 % 2.0 AS c,CAST('-9223372036854775808' AS bigint) % -1 AS edge,CAST('18446744073709551615' AS uint64) % CAST('10' AS uint64) AS wide,NULL % 2 AS missing FROM '{source}' LIMIT 1"
        ),
        &json!([{"a":-2,"b":2,"c":1.5,"edge":0,"wide":5,"missing":null}]),
    );
    let prepared = prepare(
        &format!("SELECT value % 0 AS bad FROM '{source}'"),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |leaf| DatasetUri::new(leaf.path.to_string_lossy()),
    )
    .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn native_workload_sql_text_kernels_preserve_null_and_format_semantics() {
    let source = fixture();
    verify(
        &format!(
            "SELECT SUM(CASE WHEN CAST(JSON_EXTRACT('{{\"flag\":true}}','$.flag') AS boolean) THEN value ELSE 0 END) AS total FROM '{source}' WHERE TRY_STRPTIME(CASE WHEN value=1 THEN 'bad' ELSE '2024-02-29T00:00:00Z' END,'%Y-%m-%dT%H:%M:%SZ') IS NOT NULL AND TRY_CAST('bad' AS bigint) IS NULL"
        ),
        &json!([{"total":14.0}]),
    );
    verify(
        &format!(
            "SELECT TIMESTAMP_YEAR(TRY_STRPTIME(CASE WHEN value=1 THEN '2024-02-29T12:34:56Z' ELSE '2023-02-29T12:34:56Z' END,'%Y-%m-%dT%H:%M:%SZ')) AS year FROM '{source}' LIMIT 2"
        ),
        &json!([{"year":2024},{"year":null}]),
    );
    verify(
        &format!(
            "SELECT TIMESTAMP_YEAR(STRPTIME('2024-02-29','%Y-%m-%d')) AS year,TIMESTAMP_HOUR(STRPTIME('2024-02-29 12:34:56','%Y-%m-%d %H:%M:%S')) AS hour,TRY_STRPTIME('2024-02-29T12:34:56.1Z','%Y-%m-%dT%H:%M:%SZ') AS mismatch FROM '{source}' LIMIT 1"
        ),
        &json!([{"year":2024,"hour":12,"mismatch":null}]),
    );
    verify(
        &format!(
            "SELECT CAST(JSON_EXTRACT('{{\"metrics\":{{\"score\":3.5}},\"event\":{{\"flag\":true}}}}','$.metrics.score') AS double) AS score,CAST(JSON_EXTRACT('{{\"flag\":true}}','$.flag') AS boolean) AS flag,JSON_EXTRACT('{{\"a\":[true,null,\"x\"]}}','$.a[1]') AS json_null,JSON_EXTRACT('{{\"a\":[true,null,\"x\"]}}','$.a[2]') AS text,JSON_EXTRACT('{{}}','$.missing') AS missing,JSON_EXTRACT(NULL,'$') AS absent FROM '{source}' LIMIT 1"
        ),
        &json!([{"score":3.5,"flag":true,"json_null":"null","text":"\"x\"","missing":null,"absent":null}]),
    );
    verify(
        &format!(
            "SELECT JSON_EXTRACT('{{\"n\":18446744073709551617000000000001,\"a\":[1,  2]}}','$.n') AS exact,JSON_EXTRACT('{{\"a\":[1,  2]}}','$.a') AS spacing FROM '{source}' LIMIT 1"
        ),
        &json!([{"exact":"18446744073709551617000000000001","spacing":"[1,  2]"}]),
    );
    verify(
        &format!(
            "SELECT CAST(value AS varchar) AS text,CAST(NULL AS VARCHAR) AS absent FROM '{source}' WHERE CAST(value AS varchar) = '1'"
        ),
        &json!([{"text":"1","absent":null}]),
    );
    for expression in [
        "JSON_EXTRACT('{}','$.a[*]')",
        "TRY_STRPTIME('bad','%Q')",
        "JSON_EXTRACT('{}',CAST(value AS varchar))",
    ] {
        assert!(
            prepare(
                &format!("SELECT {expression} AS bad FROM '{source}' LIMIT 0"),
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                |leaf| DatasetUri::new(leaf.path.to_string_lossy())
            )
            .is_err(),
            "{expression}"
        );
    }
    for expression in [
        "JSON_EXTRACT('malformed','$')",
        "STRPTIME('2023-02-29','%Y-%m-%d')",
    ] {
        let prepared = prepare(
            &format!("SELECT {expression} AS bad FROM '{source}'"),
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |leaf| DatasetUri::new(leaf.path.to_string_lossy()),
        )
        .unwrap();
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err(),
            "{expression}"
        );
    }
}
