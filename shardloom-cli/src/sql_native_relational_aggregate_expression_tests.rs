use super::*;

#[test]
fn native_typed_reductions_sql_untyped_null_projection_admits_direct_composed_and_empty() {
    let source = fixture();
    for input in [
        format!("'{source}'"),
        format!("(SELECT * FROM '{source}' LIMIT 2) AS q"),
    ] {
        verify(
            &format!("SELECT NULL AS missing FROM {input} LIMIT 2"),
            &json!([{"missing":null},{"missing":null}]),
        );
        verify(
            &format!("SELECT NULL AS missing FROM {input} LIMIT 0"),
            &json!([]),
        );
    }
    assert_eq!(
        source_count("SELECT NULL AS missing FROM '/missing/null.vortex' LIMIT 0").unwrap(),
        1
    );
}

#[test]
fn native_typed_reductions_sql_computed_arguments_nulls_constants_and_lazy_branches() {
    let source = fixture();
    verify(
        &format!(
            "SELECT SUM(value + metric) AS total,AVG(value * 2) AS mean,MIN(-value) AS smallest,MAX(value + 1) AS largest,COUNT(1) AS rows,COUNT(NULL) AS missing,COUNT(DISTINCT NULL) AS distinct_missing,COUNT(DISTINCT CASE WHEN value < 4 THEN value-value ELSE NULL END) AS distinct_value FROM '{source}'"
        ),
        &json!([{"total":165.0,"mean":6.0,"smallest":-5,"largest":6,"rows":5,"missing":0,"distinct_missing":0,"distinct_value":1}]),
    );
    verify(
        &format!(
            "SELECT SUM(CASE WHEN value > 0 THEN value ELSE 1/0 END) AS total,COUNT(COALESCE(NULL,NULL)) AS erased,COUNT(CASE WHEN value=1 THEN NULL ELSE NULL END) AS absent FROM '{source}'"
        ),
        &json!([{"total":15.0,"erased":0,"absent":0}]),
    );
    verify(
        &format!(
            "SELECT COUNT(1) AS rows,COUNT(NULL) AS missing,SUM(value+1) AS total FROM (SELECT value FROM '{source}' LIMIT 0) AS empty"
        ),
        &json!([{"rows":0,"missing":0,"total":null}]),
    );
}

#[test]
fn native_typed_reductions_sql_decimal_results_have_exact_declared_scales() {
    let source = fixture();
    verify(
        &format!(
            "SELECT SUM(CAST(value AS decimal128(12,2)) + CAST('0.50' AS decimal128(3,2))) AS total,AVG(CAST(value AS decimal128(12,2))) AS mean,MIN(CAST(value AS decimal128(12,2))) AS smallest,MAX(CAST(value AS decimal128(12,2))) AS largest,COUNT(DISTINCT CAST(value AS decimal128(12,2))) AS distinct_values FROM '{source}'"
        ),
        &json!([{"total":"decimal128(38,2):1750","mean":"decimal128(38,6):3000000","smallest":"decimal128(12,2):100","largest":"decimal128(12,2):500","distinct_values":5}]),
    );
}

#[test]
fn native_typed_reductions_sql_qualified_derived_grouped_having_and_private_aliases() {
    let source = fixture();
    verify(
        &format!(
            "SELECT q.renamed,SUM(q.pay+1) AS total,MIN(q.pay) AS raw FROM (SELECT value AS renamed,metric AS pay FROM '{source}') AS q GROUP BY q.renamed HAVING q.renamed>=3 ORDER BY q.renamed"
        ),
        &json!([{"q.renamed":3,"total":31.0,"raw":30},{"q.renamed":4,"total":41.0,"raw":40},{"q.renamed":5,"total":51.0,"raw":50}]),
    );
    verify(
        &format!(
            "SELECT l.value,SUM(l.metric + r.value) AS total FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value GROUP BY l.value HAVING SUM(l.metric+r.value)>30.0 ORDER BY l.value"
        ),
        &json!([{"l.value":3,"total":33.0},{"l.value":4,"total":44.0},{"l.value":5,"total":55.0}]),
    );
    verify(
        &format!(
            "SELECT SUM(q.renamed + q.pay) AS total FROM (SELECT value AS renamed,metric AS pay FROM '{source}') AS q HAVING COUNT(CASE WHEN q.renamed > 3 THEN 1 ELSE NULL END)=2"
        ),
        &json!([{"total":165.0}]),
    );
    verify(
        &format!(
            "SELECT SUM(__shardloom_expression_0 + 1) AS total FROM (SELECT value AS __shardloom_expression_0 FROM '{source}') AS q"
        ),
        &json!([{"total":20.0}]),
    );
    let parsed = parse_aggregate_projection("SUM(value+1)").unwrap().unwrap();
    verify(
        &format!("SELECT SUM(value+1) FROM '{source}'"),
        &json!([{parsed.output_name():20.0}]),
    );
}

#[test]
fn native_typed_reductions_sql_private_arguments_do_not_expand_unused_wide_input() {
    let source = fixture();
    let columns = (0..128)
        .map(|index| format!("value AS c{index}"))
        .collect::<Vec<_>>()
        .join(",");
    verify(
        &format!(
            "SELECT SUM(c0+1) AS total,MIN(c1) AS smallest FROM (SELECT {columns} FROM '{source}') AS wide"
        ),
        &json!([{"total":20.0,"smallest":1}]),
    );
}

#[test]
fn native_typed_reductions_sql_denies_invalid_empty_bindings_and_ambiguous_scopes() {
    let source = fixture();
    for statement in [
        format!("SELECT SUM(absent+1) FROM '{source}' LIMIT 0"),
        format!("SELECT COUNT(CASE WHEN value > 0 THEN 1 ELSE absent END) FROM '{source}' LIMIT 0"),
        format!(
            "SELECT SUM(value+1) FROM '{source}' AS l JOIN '{source}' AS r ON l.value=r.value LIMIT 0"
        ),
        format!("SELECT SUM(CAST(value AS binary)) FROM '{source}' LIMIT 0"),
        format!("SELECT SUM(AVG(value)) FROM '{source}' LIMIT 0"),
    ] {
        assert!(
            prepare(
                &statement,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                |path| DatasetUri::new(path.path.to_string_lossy().into_owned())
            )
            .is_err(),
            "{statement}"
        );
    }
}
