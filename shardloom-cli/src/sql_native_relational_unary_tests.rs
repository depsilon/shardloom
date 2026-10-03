use super::*;

#[test]
fn native_relational_sql_unary_all_families_preserve_transformed_input_and_downstream_stages() {
    let path = fixture();
    let input = format!("SELECT value AS n,metric AS m FROM '{path}' ORDER BY value DESC LIMIT 4");
    verify(
        &format!("SELECT n FROM TAIL(({input}), 2) AS u LIMIT 1"),
        &json!([{"n":3}]),
    );
    verify(
        &format!("SELECT SUM(n) AS total FROM DISTINCT_ROWS(({input}), 'n') AS u"),
        &json!([{"total":14.0}]),
    );
    let duplicate = format!("SELECT * FROM ({input}) AS a UNION ALL SELECT * FROM ({input}) AS b");
    for (keep, expected) in [
        ("first", json!([{"n":5},{"n":4},{"n":3},{"n":2}])),
        ("last", json!([{"n":5},{"n":4},{"n":3},{"n":2}])),
        ("false", json!([])),
    ] {
        verify(
            &format!("SELECT n FROM DROP_DUPLICATES(({duplicate}), 'n', '{keep}') AS u"),
            &expected,
        );
    }
    verify(
        &format!(
            "SELECT COUNT(*) AS total FROM DUPLICATED(({duplicate}), 'n', 'last') AS u WHERE duplicated = true"
        ),
        &json!([{"total":4}]),
    );
    let sample_input = format!("SELECT value AS n,metric AS w FROM '{path}' ORDER BY value");
    for options in [
        r#"{"n":2,"seed":7,"columns":"n"}"#,
        r#"{"fraction":0.4,"seed":7,"weights":"w","columns":"n"}"#,
    ] {
        verify(
            &format!("SELECT n FROM SAMPLE(({sample_input}), '{options}') AS u"),
            &json!([{"n":2},{"n":5}]),
        );
    }
    let rewrite = r#"{"columns":["n"],"rewrites":[{"kind":"row_number","target_column":"position","start":10}]}"#;
    verify(
        &format!("SELECT n,position FROM REWRITE(({input}), '{rewrite}') AS u WHERE position > 11"),
        &json!([{"n":3,"position":12},{"n":2,"position":13}]),
    );
    let melt = r#"{"id_columns":[],"value_columns":["n","m"],"variable_column":"field","value_column":"amount"}"#;
    let melt_input =
        format!("SELECT value AS n,value AS m FROM '{path}' ORDER BY value DESC LIMIT 4");
    verify(
        &format!(
            "SELECT field,SUM(amount) AS total FROM MELT(({melt_input}), '{melt}') AS u GROUP BY field ORDER BY field"
        ),
        &json!([{"field":"m","total":14.0},{"field":"n","total":14.0}]),
    );
    let rolling = r#"{"source_column":"n","output_column":"total","window_size":2,"min_periods":1,"aggregate":"sum"}"#;
    verify(
        &format!(
            "SELECT * FROM TAIL((SELECT * FROM ROLLING(({input}), '{rolling}') AS r), 2) AS u"
        ),
        &json!([{"total":7.0},{"total":5.0}]),
    );
}

#[test]
fn native_relational_sql_unary_column_resolution_matches_derived_scope() {
    let path = fixture();
    let input = format!(
        "SELECT l.value,r.metric FROM '{path}' AS l JOIN '{path}' AS r ON l.value = r.value"
    );
    verify(
        &format!(
            "SELECT value AS n FROM DISTINCT_ROWS(({input}), 'value') AS u ORDER BY value LIMIT 2"
        ),
        &json!([{"n":1},{"n":2}]),
    );
    verify(
        &format!(
            "SELECT value AS n FROM DROP_DUPLICATES(({input}), 'value', 'first') AS u ORDER BY value LIMIT 2"
        ),
        &json!([{"n":1},{"n":2}]),
    );
    let options = r#"{"n":2,"seed":7,"weights":"metric","columns":"value"}"#;
    verify(
        &format!("SELECT value AS n FROM SAMPLE(({input}), '{options}') AS u"),
        &json!([{"n":2},{"n":5}]),
    );
    let rewrite = r#"{"columns":["value","metric"],"rewrites":[{"kind":"mask_scalar","target_column":"value","predicate":"lt:metric:30","replacement":{"type":"int64","value":99}}]}"#;
    verify(
        &format!("SELECT value AS n FROM REWRITE(({input}), '{rewrite}') AS u LIMIT 3"),
        &json!([{"n":99},{"n":99},{"n":3}]),
    );
    for source in [
        format!("SELECT * FROM '{path}' AS l JOIN '{path}' AS r ON l.value = r.value"),
        format!("SELECT value AS renamed FROM '{path}'"),
    ] {
        let error = prepare(
            &format!("SELECT * FROM DISTINCT_ROWS(({source}), 'value') AS u"),
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |leaf| DatasetUri::new(leaf.path.to_string_lossy().into_owned()),
        )
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("ambiguous") || error.to_string().contains("not present"),
            "{error}"
        );
    }
}

#[test]
fn native_relational_sql_unary_parsing_is_inert_and_preserves_quoted_arguments() {
    let sql = "SELECT * FROM REWRITE((SELECT value FROM 'missing o''clock,(on).vortex'), '{\"columns\":[\"value\"],\"rewrites\":[{\"kind\":\"string_replace_scalar\",\"target_column\":\"value\",\"needle\":\"o''clock,(join)\",\"replacement\":\"it''s fine\"}]}') AS u";
    assert!(is_relational(sql).unwrap());
    let leaves = source_leaves(sql).unwrap();
    assert_eq!(leaves.len(), 1);
    assert_eq!(
        leaves.iter().next().unwrap().path,
        Path::new("missing o'clock,(on).vortex")
    );
    assert!(!leaves.iter().next().unwrap().declared_identifier);
    let declared = source_leaves(
        "SELECT * FROM TAIL((SELECT * FROM table_a UNION ALL SELECT * FROM table_b), 2) AS u",
    )
    .unwrap();
    assert_eq!(declared.len(), 2);
    assert!(declared.iter().all(|leaf| leaf.declared_identifier));
    let ParsedRelationQuery::Select(parsed) = ParsedRelationQuery::parse(sql).unwrap() else {
        panic!()
    };
    let ParsedRelationSource::Unary(unary) = parsed.source else {
        panic!()
    };
    let shardloom_vortex::VortexExpressionRewrite::StringReplaceScalar {
        needle,
        replacement,
        ..
    } = &unary.request.expression_projection.unwrap().rewrites[0]
    else {
        panic!()
    };
    assert_eq!(needle, "o'clock,(join)");
    assert_eq!(replacement, "it's fine");
    assert!(unary.request.source_uri.is_none());
}

#[test]
fn native_relational_sql_unary_nested_literal_tokens_are_lossless() {
    for value in ["isn't,(join)", "it's fine", "'東京'", "a''b", ""] {
        let literal = value.replace('\'', "''");
        assert_eq!(
            parse_predicate(&format!("label = '{literal}'")).unwrap(),
            ParsedPredicate::Compare {
                column: "label".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8(value.into()),
            }
        );
        let sql = format!(
            "SELECT label FROM TAIL((SELECT * FROM (SELECT * FROM 'missing o''clock,(join).vortex' LIMIT 2) AS limited WHERE label = '{literal}'), 1) AS u"
        );
        assert!(is_relational(&sql).unwrap());
        let leaves = source_leaves(&sql).unwrap();
        assert_eq!(leaves.len(), 1);
        assert_eq!(
            leaves.iter().next().unwrap().path,
            Path::new("missing o'clock,(join).vortex")
        );
    }
    assert!(parse_predicate("label = 'isn't escaped'").is_err());
}

#[test]
fn native_relational_sql_unary_quoted_projection_and_membership_keep_literal_values() {
    let source = fixture();
    for value in ["isn't,(join)", "it's fine", "'東京'", "a''b", ""] {
        let literal = value.replace('\'', "''");
        let sql = format!(
            "SELECT label FROM TAIL((SELECT '{literal}' AS label FROM '{source}' LIMIT 1), 1) AS u WHERE label IN ('{literal}')"
        );
        verify(&sql, &json!([{"label":value}]));
    }
}

#[test]
fn native_relational_sql_unary_malformed_arguments_fail_before_source_resolution() {
    for expression in [
        "TAIL((SELECT * FROM missing), 0)",
        "TAIL((SELECT * FROM missing), -1)",
        "TAIL((SELECT * FROM missing), 2,)",
        "TAIL(SELECT * FROM missing, 2)",
        "TAIL((SELECT * FROM missing), 2) extra",
        "MISSING((SELECT * FROM missing), 2)",
        "DROP_DUPLICATES((SELECT * FROM missing), '*', 'maybe')",
        "SAMPLE((SELECT * FROM missing), '{\"n\":1,\"fraction\":0.2}')",
        "SAMPLE((SELECT * FROM missing), '{\"n\":0}')",
        "SAMPLE((SELECT * FROM missing), '{\"n\":1,\"n\":2}')",
        "SAMPLE((SELECT * FROM missing), '{\"n\":1,\"other\":2}')",
        "SAMPLE((SELECT * FROM missing), '{\"fraction\":1.1}')",
        "MELT((SELECT * FROM missing), '{\"value_columns\":[\"a\"],\"value_vars\":[\"b\"]}')",
        "MELT((SELECT * FROM missing), '{\"value_columns\":[\"a\"],\"value_columns\":[\"b\"]}')",
        "ROLLING((SELECT * FROM missing), '{\"source_column\":\"a\",\"unknown\":true}')",
        "REWRITE((SELECT * FROM missing), '{\"columns\":\"*\",\"rewrites\":[]}')",
    ] {
        let statement = format!("SELECT * FROM {expression} AS u");
        assert!(
            prepare(
                &statement,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                |_| panic!("malformed operation opened input: {statement}")
            )
            .is_err(),
            "{statement}"
        );
    }
    assert!(is_relational("SELECT * FROM TAIL((SELECT * FROM missing), 2)").is_err());
}

#[test]
fn native_relational_sql_unary_mixed_melt_does_not_widen_scalar_admission() {
    let path = fixture();
    let payload = r#"{"id_columns":[],"value_columns":["value","metric"],"variable_column":"field","value_column":"amount"}"#;
    let error = prepare(
        &format!("SELECT * FROM MELT((SELECT * FROM '{path}'), '{payload}') AS u"),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |leaf| DatasetUri::new(leaf.path.to_string_lossy().into_owned()),
    )
    .err()
    .expect("mixed scalar domains produce Variant, outside this composition contract");
    assert!(
        error
            .to_string()
            .contains("requires bool, integer, F32/F64 or UTF8"),
        "{error}"
    );
}
