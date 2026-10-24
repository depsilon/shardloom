use super::*;

#[test]
fn native_typed_arithmetic_group_keys_preserve_names_and_bind_selected_expressions() {
    for (projection, grouping, name) in [
        ("ip - 3", "ip-3", "ip_minus3"),
        ("ip+2", "ip + 2", "ip_plus2"),
        ("ip - 3 AS shifted", "ip-3", "shifted"),
        ("ip-3 AS shifted", "ip - 3", "shifted"),
        ("ip - 3 AS shifted", "shifted", "shifted"),
    ] {
        let parsed = parse_sql_local_source_statement(&format!(
            "SELECT ip,{projection},COUNT(*) AS n FROM 'missing.vortex' GROUP BY ip,{grouping} ORDER BY {grouping} DESC NULLS LAST,COUNT(*) LIMIT 10"
        )).unwrap();
        assert_eq!(parsed.group_by, ["ip", name]);
        let order = parsed.order_by.unwrap();
        assert_eq!(order.keys[0].column, name);
        assert_eq!(order.keys[0].direction, SortDirection::Desc);
        assert_eq!(order.keys[0].null_ordering, Some(SortNullOrdering::Last));
        assert_eq!(order.keys[1].column, "n");
    }
    assert!(
        parse_sql_local_source_statement(
            "SELECT ip,COUNT(*) AS n FROM 'missing.vortex' GROUP BY ip-3 LIMIT 10"
        )
        .is_err()
    );
}

fn computed_columns(aggregate: &ParsedAggregate) -> &[String] {
    let ParsedAggregateArgument::Computed { source_columns, .. } = &aggregate.argument else {
        panic!("expected computed aggregate argument");
    };
    source_columns
}

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
        assert_eq!(computed_columns(&aggregate), columns);
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
    assert!(
        matches!(&null.argument, ParsedAggregateArgument::Computed { raw, .. } if raw == "NULL")
    );
    assert_ne!(null.argument, ParsedAggregateArgument::All);
}

#[test]
fn native_typed_reductions_parse_having_and_all_dependency_consumers() {
    let parsed = parse_sql_local_source_statement(
        "SELECT category,SUM(amount + fee) AS total FROM 'input.csv' GROUP BY category HAVING AVG(price + tax) > 1 LIMIT 10",
    ).unwrap();
    assert_eq!(computed_columns(&parsed.aggregates[0]), ["amount", "fee"]);
    assert_eq!(
        computed_columns(&parsed.having_aggregates[0]),
        ["price", "tax"]
    );
    let argument = parse_aggregate_projection("SUM(value + 1)")
        .unwrap()
        .unwrap();
    let reserved = format!(
        "__having_{}_1",
        sanitize_having_aggregate_alias(&argument.output_name())
    );
    let (rewritten, having) = rewrite_having_aggregate_predicate(
        "SUM(value + 1) > 0",
        &[],
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

#[test]
fn native_having_reuses_selected_aggregate_values_without_hidden_updates() {
    for (selected, predicate, output) in [
        ("COUNT(*) AS rows", "count(*) > 1", "rows"),
        ("COUNT(*)", "COUNT(*) > 1", "count_all"),
        ("SUM(value + 1) AS total", "sum(value+1) > 1", "total"),
        (
            "COUNT(DISTINCT value) AS distinct_values",
            "count(DISTINCT value) > 1",
            "distinct_values",
        ),
    ] {
        let parsed = parse_sql_local_source_statement(&format!(
            "SELECT key,{selected} FROM 'missing.vortex' GROUP BY key HAVING {predicate} LIMIT 10"
        ))
        .unwrap();
        assert!(parsed.having_aggregates.is_empty(), "{selected}");
        assert_eq!(parsed.having.columns(), [output], "{selected}");
    }
}

#[test]
fn native_having_reuses_hidden_values_without_merging_distinct_arguments() {
    let parsed = parse_sql_local_source_statement(
        "SELECT key,COUNT(value) AS n,COUNT(DISTINCT value) AS distinct_values FROM 'missing.vortex' GROUP BY key HAVING COUNT(*) > 1 AND COUNT(value) > 0 AND COUNT(DISTINCT value) > 0 AND COUNT(*) < 10 LIMIT 10",
    ).unwrap();
    assert_eq!(parsed.having_aggregates.len(), 1);
    assert_eq!(
        parsed.having_aggregates[0].argument,
        ParsedAggregateArgument::All
    );
    assert_eq!(
        parsed.having_aggregates[0].output_name(),
        "__having_count_all_1"
    );
    assert!(parsed.having.columns().contains(&"n"));
    assert!(parsed.having.columns().contains(&"distinct_values"));

    for predicate in [
        "SUM(value + 2) > 1",
        "SUM(value + 1.0) > 1",
        "AVG(value + 1) > 1",
    ] {
        let parsed = parse_sql_local_source_statement(&format!(
            "SELECT key,SUM(value + 1) AS total FROM 'missing.vortex' GROUP BY key HAVING {predicate} LIMIT 10"
        )).unwrap();
        assert_eq!(parsed.having_aggregates.len(), 1, "{predicate}");
        assert_ne!(parsed.having_aggregates[0].output_name(), "total");
    }
}

#[test]
fn native_having_reuse_keeps_subquery_aggregates_in_their_own_scope() {
    let parsed = parse_sql_local_source_statement(
        "SELECT key,COUNT(*) AS rows FROM 'missing.vortex' GROUP BY key HAVING COUNT(*) > ALL (SELECT COUNT(*) FROM 'other.vortex' LIMIT 1) LIMIT 10",
    ).unwrap();
    assert_eq!(parsed.having_aggregates, Vec::<ParsedAggregate>::new());
    let ParsedPredicate::QuantifiedSubquery {
        column, subquery, ..
    } = parsed.having
    else {
        panic!("expected quantified HAVING subquery");
    };
    assert_eq!(column, "rows");
    let inner = subquery.projected_plan.expect("inner aggregate plan");
    assert_eq!(inner.aggregates.len(), 1);
    assert_eq!(inner.aggregates[0].output_name(), "count_all");
    assert_eq!(inner.having_aggregates, Vec::<ParsedAggregate>::new());
}
