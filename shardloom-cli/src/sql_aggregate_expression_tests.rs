use super::*;

#[test]
fn native_typed_reductions_parse_arguments_dependencies_and_stable_names() {
    for (raw, columns) in [
        ("SUM(a + b * 2)", vec!["a", "b"]),
        (
            "COUNT(DISTINCT CASE WHEN a > 0 THEN b ELSE c END)",
            vec!["a", "b", "c"],
        ),
        ("AVG(CAST(f.amount AS decimal128(12,2)))", vec!["f.amount"]),
        ("COUNT(NULL)", vec![]),
        ("COUNT(1)", vec![]),
    ] {
        let aggregate = parse_aggregate_projection(raw).unwrap().unwrap();
        assert_eq!(aggregate.source_columns(), columns);
        assert!(matches!(
            aggregate.argument,
            ParsedAggregateArgument::Computed { .. }
        ));
        let name = aggregate.output_name();
        assert!(name.len() < 96);
        validate_sql_identifier(&name).unwrap();
        assert_eq!(
            name,
            parse_aggregate_projection(raw)
                .unwrap()
                .unwrap()
                .output_name()
        );
        assert_ne!(name, "count_all");
        assert!(aggregate.required_column().is_err());
        assert!(evaluate_scalar_aggregate(&aggregate, &[], &[]).is_err());
    }
    for (raw, name) in [
        ("SUM(value)", "sum_value"),
        ("COUNT(DISTINCT f.value)", "count_distinct_f.value"),
        ("COUNT(*)", "count_all"),
    ] {
        assert_eq!(
            parse_aggregate_projection(raw)
                .unwrap()
                .unwrap()
                .output_name(),
            name
        );
    }
    let null = parse_aggregate_projection("COUNT(NULL)").unwrap().unwrap();
    assert_eq!(null.label(), "count(NULL)");
    assert_ne!(null.argument, ParsedAggregateArgument::All);
}

#[test]
fn native_typed_reductions_parse_having_and_all_dependency_consumers() {
    let parsed = parse_sql_local_source_statement(
        "SELECT category,SUM(amount + fee) AS total FROM 'input.csv' GROUP BY category HAVING AVG(price + tax) > 1 LIMIT 10",
    ).unwrap();
    assert_eq!(parsed.aggregates[0].source_columns(), ["amount", "fee"]);
    assert_eq!(
        parsed.having_aggregates[0].source_columns(),
        ["price", "tax"]
    );
    let header = ["category", "amount", "fee", "price", "tax"].map(str::to_owned);
    validate_aggregate_source_columns(&parsed, &header).unwrap();
    assert!(validate_aggregate_source_columns(&parsed, &header[..4]).is_err());
    let refs = join_left_existence_source_refs(&parsed);
    for name in ["category", "amount", "fee", "price", "tax"] {
        assert!(refs.contains(name));
    }
    let read = source_read_plan_for_sql(&parsed);
    let expected = source_read_plan_for_sql(
        &parse_sql_local_source_statement(
            "SELECT category,amount,fee,price,tax FROM 'input.csv' LIMIT 10",
        )
        .unwrap(),
    );
    assert_eq!(read, expected);
    let argument = parse_aggregate_projection("SUM(value + 1)")
        .unwrap()
        .unwrap();
    let reserved = format!(
        "__having_{}_1",
        sanitize_having_aggregate_alias(&argument.output_name())
    );
    let (rewritten, having) = rewrite_having_aggregate_predicate(
        "SUM(value + 1) > 0",
        BTreeSet::from([reserved.clone()]),
    )
    .unwrap();
    assert_ne!(having[0].output_name(), reserved);
    assert!(rewritten.starts_with(&having[0].output_name()));
}

#[test]
fn native_typed_reductions_reject_ambiguous_or_unimplemented_aggregate_forms() {
    for raw in [
        "SUM()",
        "COUNT(DISTINCT)",
        "COUNT(DISTINCT *)",
        "AVG(DISTINCT value)",
        "SUM(DISTINCT value)",
        "SUM(value,other)",
        "SUM(AVG(value))",
        "COUNT(fetch_url('https://example.invalid'))",
    ] {
        assert!(parse_aggregate_projection(raw).is_err(), "{raw}");
    }
    let large = format!("COUNT('{}')", "x".repeat(65_537));
    assert!(parse_aggregate_projection(&large).is_err());
    assert!(
        parse_sql_local_source_statement("SELECT SUM(value+1),SUM(value+1) FROM 'input.csv'")
            .is_err()
    );
}
