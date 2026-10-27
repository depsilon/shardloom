use super::*;

#[test]
fn aggregate_filter_identity_dependencies_and_syntax_are_explicit() {
    let parse = |sql| parse_aggregate_projection(sql).unwrap().unwrap();
    let filtered = parse("SUM(a) FILTER (WHERE b > 2)");
    let same = parse("SUM(a) FILTER (WHERE b>2) AS renamed");
    assert!(filtered.same_value(&same));
    assert_eq!(filtered.columns().collect::<Vec<_>>(), ["a", "b"]);
    for other in [
        parse("SUM(a)"),
        parse("SUM(a) FILTER (WHERE b > 3)"),
        parse("SUM(DISTINCT a) FILTER (WHERE b > 2)"),
    ] {
        assert!(!filtered.same_value(&other));
        assert_ne!(filtered.output_name(), other.output_name());
    }
    assert!(parse("COUNT(*) FILTER (WHERE enabled)").filter.is_some());
    assert!(
        parse("STRING_AGG(a,'FILTER (WHERE bad)' ORDER BY b) FILTER (WHERE enabled)")
            .filter
            .is_some()
    );
    for invalid in [
        "SUM(a) FILTER ()",
        "SUM(a) FILTER (WHERE)",
        "SUM(a) FILTER (b)",
        "SUM(a) FILTER WHERE b",
        "SUM(a) FILTER (WHERE b) FILTER (WHERE c)",
        "SUM(a) trailing",
    ] {
        assert!(parse_aggregate_projection(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn ordered_selector_arguments_preserve_local_order_identity_dependencies_and_arity() {
    let parse = |sql| parse_aggregate_projection(sql).unwrap().unwrap();
    let plain = parse("FIRST(a)");
    let ordered = parse("FIRST(a ORDER BY b ASC NULLS LAST,c DESC)");
    let same = parse("FIRST(a ORDER BY b NULLS LAST, c DESC) AS renamed");
    let descending = parse("FIRST(a ORDER BY b DESC NULLS LAST,c DESC)");
    let nulls_first = parse("FIRST(a ORDER BY b NULLS FIRST,c DESC)");
    assert!(ordered.same_value(&same));
    assert_eq!(ordered.columns().collect::<Vec<_>>(), ["a", "b", "c"]);
    for different in [plain, descending, nulls_first] {
        assert!(!ordered.same_value(&different));
        assert_ne!(ordered.output_name(), different.output_name());
    }
    let paired = parse("ARG_MIN(a, b ORDER BY c DESC NULLS FIRST)");
    assert_eq!(paired.arguments().count(), 2);
    assert_eq!(paired.columns().collect::<Vec<_>>(), ["a", "b", "c"]);
    assert!(parse("LAST('order by')").order_by.is_none());
    assert!(
        parse("FIRST(CAST(a AS INT64) ORDER BY b)")
            .order_by
            .is_some()
    );
    for invalid in [
        "FIRST(a,b)",
        "ARG_MIN(a)",
        "ARG_MAX(a,b,c)",
        "FIRST(*)",
        "LAST(DISTINCT a)",
        "ARG_MIN(DISTINCT a,b)",
        "FIRST(a ORDER BY)",
        "FIRST(a ORDER BY b,b)",
        "FIRST(a ORDER BY b NULLS MIDDLE)",
        "SUM(a ORDER BY b)",
        "CORR(a,b ORDER BY c)",
    ] {
        assert!(parse_aggregate_projection(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn ordered_collection_arguments_preserve_separator_identity_and_dependencies() {
    let parse = |sql| parse_aggregate_projection(sql).unwrap().unwrap();
    let list = parse("LIST(a ORDER BY b)");
    let array = parse("ARRAY_AGG(a ORDER BY b)");
    assert_eq!(list.function, AggregateFunction::List);
    assert_eq!(array.function, AggregateFunction::List);
    assert!(list.same_value(&array));
    assert_eq!(list.columns().collect::<Vec<_>>(), ["a", "b"]);

    let empty = parse("STRING_AGG(a, '')");
    let pipe = parse("STRING_AGG(a, '|')");
    let quoted_unicode = parse("STRING_AGG(a, '''東京')");
    assert_eq!(empty.separator.as_deref(), Some(""));
    assert_eq!(pipe.separator.as_deref(), Some("|"));
    assert_eq!(quoted_unicode.separator.as_deref(), Some("'東京"));
    assert!(empty.columns().eq(["a"]));
    let ordered = parse("STRING_AGG(a, '/' ORDER BY b)");
    assert_eq!(ordered.columns().collect::<Vec<_>>(), ["a", "b"]);
    for other in [pipe, quoted_unicode, ordered] {
        assert!(!empty.same_value(&other));
        assert_ne!(empty.output_name(), other.output_name());
    }
    for invalid in [
        "STRING_AGG(a)",
        "STRING_AGG(a, '|', b)",
        "STRING_AGG(a, b)",
        "LIST(a, '|')",
    ] {
        assert!(parse_aggregate_projection(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn exact_quantile_arguments_keep_probability_out_of_dependencies_and_identity_distinct() {
    let parse = |sql| parse_aggregate_projection(sql).unwrap().unwrap();
    let low = parse("QUANTILE_CONT(a+1, 0.25)");
    let same = parse("QUANTILE_CONT(a + 1, 2.5e-1) AS other");
    let high = parse("QUANTILE_CONT(a+1, 0.75)");
    assert!(low.same_value(&same));
    assert!(!low.same_value(&high));
    assert_ne!(low.output_name(), high.output_name());
    assert_eq!(low.arguments().count(), 1);
    assert!(low.secondary_argument.is_none());
    assert_eq!(
        low.quantile_level.unwrap(),
        VortexQuantileLevel::new(0.25).unwrap()
    );
    assert!(parse("QUANTILE_DISC(a,-0.0)").same_value(&parse("QUANTILE_DISC(a,0)")));
    assert_eq!(
        parse("QUANTILE_DISC(a,1)").output_name(),
        parse("QUANTILE_DISC(a,1.0)").output_name()
    );
    assert!(parse("MEDIAN(a)").quantile_level.is_none());
    for invalid in [
        "QUANTILE_CONT(a)",
        "QUANTILE_DISC(a,0.5,1)",
        "MEDIAN(a,0.5)",
        "QUANTILE_CONT(a,b)",
        "QUANTILE_CONT(a,1/2)",
        "QUANTILE_CONT(a,NULL)",
        "QUANTILE_CONT(a,true)",
        "QUANTILE_CONT(a,'0.5')",
        "QUANTILE_CONT(a,NaN)",
        "QUANTILE_CONT(a,1e999)",
        "QUANTILE_CONT(a,-0.1)",
        "QUANTILE_CONT(a,1.1)",
        "QUANTILE_DISC(DISTINCT a,0.5)",
        "QUANTILE_CONT(*,0.5)",
    ] {
        assert!(parse_aggregate_projection(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn native_statistical_arguments_preserve_both_dependencies_names_and_arity() {
    let parsed = parse_aggregate_projection("COVAR_SAMP(a+1, COALESCE(b,c))")
        .unwrap()
        .unwrap();
    assert_eq!(parsed.function, AggregateFunction::CovarSamp);
    let columns = parsed
        .arguments()
        .flat_map(|argument| match argument {
            ParsedAggregateArgument::Computed { source_columns, .. } => source_columns.clone(),
            ParsedAggregateArgument::Column(name) => vec![name.clone()],
            ParsedAggregateArgument::All => vec![],
        })
        .collect::<Vec<_>>();
    assert_eq!(columns, ["a", "b", "c"]);
    let same = parse_aggregate_projection("COVAR_SAMP(a + 1,COALESCE(b, c)) AS other")
        .unwrap()
        .unwrap();
    assert!(parsed.same_value(&same));
    let different = parse_aggregate_projection("COVAR_SAMP(a+1, COALESCE(b,d))")
        .unwrap()
        .unwrap();
    assert!(!parsed.same_value(&different));
    assert_ne!(parsed.output_name(), different.output_name());
    for invalid in [
        "CORR(a)",
        "CORR(a,b,c)",
        "VAR_POP(a,b)",
        "COVAR_POP(*)",
        "STDDEV_POP(*)",
    ] {
        assert!(parse_aggregate_projection(invalid).is_err(), "{invalid}");
    }
}

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
        "VAR_POP(DISTINCT value)",
        "COVAR_POP(DISTINCT value,other)",
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
