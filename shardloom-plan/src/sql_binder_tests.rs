// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::plan_ir::{PlanId, PlanLayer, PlanValidationStatus};

#[test]
fn binds_single_source_filter_aggregate_sort_limit_and_native_document() {
    let sql = "SELECT customer_id, COUNT(*) AS orders, SUM(amount) FILTER (WHERE amount > 0) AS total \
               FROM 'data/orders.vortex' \
               WHERE region = 'us-east' AND status IS DISTINCT FROM 'cancelled' \
               GROUP BY customer_id HAVING COUNT(*) >= :min_orders \
               ORDER BY total DESC NULLS LAST LIMIT 25 OFFSET 5";
    let bound = bind_sql(sql).expect("valid SQL should bind");
    assert_eq!(
        bound.named_parameters,
        BTreeSet::from(["min_orders".to_string()])
    );
    assert_eq!(
        bound.source_refs,
        vec![SqlSourceRef {
            reference: "data/orders.vortex".to_string(),
            quoted_path: true,
        }]
    );
    let doc = bound
        .to_native_plan_document(PlanId::new("sql.orders").unwrap())
        .expect("document builds");
    assert_eq!(doc.layer, PlanLayer::Logical);
    assert_eq!(doc.validation.status, PlanValidationStatus::Valid);
    assert!(!doc.validation.fallback_execution_allowed);
    assert!(
        doc.nodes
            .iter()
            .any(|n| n.label.starts_with("scan:data/orders.vortex"))
    );
    assert!(
        doc.nodes
            .iter()
            .any(|n| n.kind == NativePlanNodeKind::Filter)
    );
    assert!(
        doc.nodes
            .iter()
            .any(|n| n.kind == NativePlanNodeKind::Aggregate)
    );
    assert!(doc.nodes.iter().any(|n| n.kind == NativePlanNodeKind::Sort));
    assert!(
        doc.nodes
            .iter()
            .any(|n| n.kind == NativePlanNodeKind::Limit)
    );
}

#[test]
fn binds_asof_left_join_tolerance_and_cte_materialization() {
    let sql = "WITH quotes AS MATERIALIZED (SELECT ts, sym, bid FROM 'quotes.vortex') \
               SELECT t.id, q.bid FROM 'trades.vortex' t \
               ASOF LEFT JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym TOLERANCE 500 \
               LIMIT 100";
    let bound = bind_sql(sql).expect("ASOF LEFT JOIN + TOLERANCE binds");
    let LogicalRelation::WithCte { ctes, body, .. } = &bound.root else {
        panic!("expected CTE wrapper, got {:?}", bound.root);
    };
    assert_eq!(ctes.len(), 1);
    assert_eq!(ctes[0].materialized, Some(true));
    let LogicalRelation::Limit { input, .. } = body.as_ref() else {
        panic!("expected Limit");
    };
    let LogicalRelation::Project { input, .. } = input.as_ref() else {
        panic!("expected Project");
    };
    let LogicalRelation::Join {
        join_type,
        tolerance,
        match_condition,
        on,
        ..
    } = input.as_ref()
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::AsOfLeft);
    assert_eq!(*tolerance, Some(500));
    assert!(match_condition.is_some());
    assert!(on.is_some());
}

#[test]
fn binds_windows_sets_subqueries_and_typed_literals() {
    let sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rk, \
               CAST(amount AS DECIMAL(18, 2)) AS amt, DATE '2026-11-23' AS dt \
               FROM 'emp.vortex' WHERE id IN (SELECT manager_id FROM 'mgr.vortex') \
               UNION ALL SELECT id, 1 AS rk, CAST(0 AS DECIMAL(18, 2)) AS amt, DATE '2026-11-23' AS dt FROM 'contractors.vortex' LIMIT 50";
    let bound = bind_sql(sql).expect("window + set + subquery should bind");
    assert_eq!(bound.source_refs.len(), 3);
    let summary = inspect_sql(sql).expect("inspect works");
    assert!(summary.is_relational_candidate);
    assert!(summary.has_top_level_limit);
}

#[test]
fn converts_scalar_logical_expr_to_core_expression() {
    let sql =
        "SELECT (price + 10) * 2 AS adj, CAST(score AS INT64) AS s FROM 'items.vortex' LIMIT 1";
    let bound = bind_sql(sql).expect("binds");
    let LogicalRelation::Limit { input, .. } = &bound.root else {
        panic!("expected limit");
    };
    let LogicalRelation::Project { items, .. } = input.as_ref() else {
        panic!("expected project");
    };
    let LogicalProjectionItem::Expr { expr, .. } = &items[0] else {
        panic!("expected expr");
    };
    let core_expr = expr
        .try_into_core_expression("proj.0")
        .expect("core conversion");
    assert!(matches!(
        core_expr.kind,
        shardloom_core::ExpressionKind::Binary { .. }
    ));
    let LogicalProjectionItem::Expr {
        expr: cast_expr, ..
    } = &items[1]
    else {
        panic!("expected cast expr");
    };
    let core_cast = cast_expr
        .try_into_core_expression("proj.1")
        .expect("cast converts");
    assert!(matches!(
        core_cast.kind,
        shardloom_core::ExpressionKind::Cast { .. }
    ));
}

#[test]
fn extracts_utf8_multiline_spans_and_retires_comment_and_quote_bugs() {
    let sql = "-- FROM 'fake_in_comment.csv'\nSELECT 'FROM ''inner.csv''' AS label, a IS NOT DISTINCT FROM b AS eq /* JOIN 'comment.csv' */\nFROM 'реальные_данные.vortex' AS t\nWHERE a = '1' AND b BETWEEN 2 AND 5 LIMIT 10";
    let spans = extract_source_ref_spans(sql);
    assert_eq!(
        spans.len(),
        1,
        "must ignore comments, strings, IS NOT DISTINCT FROM"
    );
    assert_eq!(spans[0].value, "реальные_данные.vortex");
    assert!(spans[0].quoted);
    assert_eq!(
        &sql[spans[0].start..spans[0].end],
        "'реальные_данные.vortex'"
    );

    let where_idx = find_top_level_keyword(sql, "where")
        .unwrap()
        .expect("WHERE found");
    let after_where = &sql
        [where_idx + "WHERE".len()..find_top_level_keyword(sql, "limit").unwrap().unwrap()]
        .trim();
    assert_eq!(
        split_top_level_conjunctions(after_where),
        vec!["a = '1'", "b BETWEEN 2 AND 5"]
    );
}

#[test]
fn token_helpers_handle_csv_binary_comparison_cte_and_single_source_shape() {
    assert_eq!(
        split_top_level_csv("a, CONCAT(b, ',', 'x''y'), arr[1, 2]").unwrap(),
        vec!["a", "CONCAT(b, ',', 'x''y')", "arr[1, 2]"]
    );

    // Binary operator selects rightmost top-level low-precedence op while skipping unary sign & scientific exponent.
    assert_eq!(
        find_top_level_binary_operator("-1.5e-3 + b - c", &['+', '-']).unwrap(),
        Some((12, '-'))
    );
    assert_eq!(
        find_top_level_binary_operator("-a", &['+', '-']).unwrap(),
        None
    );

    assert_eq!(
        find_top_level_comparison_operator("x + 1 >= 42").unwrap(),
        Some((6, ">=", shardloom_core::ComparisonOp::GtEq))
    );
    assert!(find_top_level_comparison_operator("x = 1 = 2").is_err());

    let ctes = extract_cte_definitions(
        "WITH RECURSIVE step_up(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM step_up WHERE n < 3) SELECT n FROM step_up LIMIT 5",
    )
    .unwrap();
    assert!(ctes.recursive);
    assert_eq!(ctes.definitions.len(), 1);
    assert_eq!(ctes.definitions[0].name, "step_up");
    assert_eq!(ctes.definitions[0].columns, vec!["n"]);
    assert!(ctes.definitions[0].materialized);
    assert_eq!(
        ctes.definitions[0].body_sql,
        "SELECT 1 UNION ALL SELECT n + 1 FROM step_up WHERE n < 3"
    );
    assert_eq!(ctes.tail_sql, "SELECT n FROM step_up LIMIT 5");

    let shape = extract_single_source_shape(
        "SELECT id, COUNT(*) AS n FROM 'events.vortex' WHERE id >= 10 GROUP BY id HAVING n > 1 ORDER BY id DESC LIMIT 5 OFFSET 1",
    )
    .expect("single-source shape");
    assert_eq!(shape.source_ref, "events.vortex");
    assert!(!shape.source_is_identifier);
    assert_eq!(shape.limit.as_deref(), Some("5"));
    assert_eq!(shape.offset.as_deref(), Some("1"));
}

#[test]
fn rejects_dml_ddl_positional_params_unclosed_quotes_and_excessive_nesting_deterministically() {
    for bad in [
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE x (id INT)",
        "SELECT * FROM 'a.vortex' WHERE id = ?",
        "SELECT * FROM 'a.vortex' WHERE id = $1",
        "SELECT * FROM 'unclosed",
        "SELECT * FROM 'a.vortex'; SELECT 1",
    ] {
        let err = bind_sql(bad).expect_err(bad);
        let msg = err.to_string();
        assert!(
            msg.contains("fallback"),
            "expected explicit no-fallback msg for `{bad}`: {msg}"
        );
    }

    let deep = format!("{}1{}", "(".repeat(30), ")".repeat(30));
    let err = find_top_level_keyword(&deep, "from").expect_err("depth > 24 rejected");
    assert!(err.to_string().contains("24 nesting levels"));
}

#[test]
fn inspect_sql_json_reports_source_free_generator_and_limits() {
    let values_json = inspect_sql_json("VALUES (1, 'a'), (2, 'b') LIMIT 5").unwrap();
    assert!(values_json.contains("\"statement_kind\":\"values\""));
    assert!(values_json.contains("\"is_source_free\":true"));
    assert!(values_json.contains("\"top_level_limit\":\"LIMIT 5\""));

    let gen_json =
        inspect_sql_json("SELECT value FROM generate_series(1, 5) WHERE value > 2").unwrap();
    assert!(gen_json.contains("\"is_source_free\":true"));
    assert!(gen_json.contains("\"is_generator\":true"));
}

#[test]
fn handles_single_source_shape_bounds_aliases_from_skipping_and_csv_slices() {
    assert_eq!(
        find_source_from_clause("SELECT a IS DISTINCT FROM b FROM 'x.vortex'").unwrap(),
        Some(28)
    );
    assert_eq!(
        find_source_from_clause("SELECT a IS NOT DISTINCT FROM b FROM 'x.vortex'").unwrap(),
        Some(32)
    );
    assert!(
        extract_single_source_shape("SELECT a FROM 'x.vortex' LIMIT 18446744073709551616")
            .is_none()
    );
    assert!(extract_single_source_shape("SELECT a FROM 'x.vortex' AS t").is_none());
    let zero_shape = extract_single_source_shape("SELECT a FROM 'x.vortex' LIMIT 00")
        .expect("leading zeroes normalize cleanly");
    assert_eq!(zero_shape.limit.as_deref(), Some("0"));
    assert_eq!(
        split_top_level_csv_slices("a, COALESCE(b, 'x,y'), c").unwrap(),
        vec!["a", "COALESCE(b, 'x,y')", "c"]
    );
}

#[test]
fn relational_candidate_detects_table_functions_and_nested_subqueries() {
    assert!(is_relational_sql_candidate(
        "SELECT * FROM SAMPLE((SELECT * FROM MELT((SELECT * FROM PIVOT((SELECT * FROM 'in.vortex' WHERE amount IS NOT NULL), '{}')) AS m)), '{}') AS sampled"
    ));
    assert!(is_relational_sql_candidate(
        "SELECT COALESCE((SELECT x FROM 'b.vortex'), 0) FROM 'a.vortex'"
    ));
    assert!(!is_relational_sql_candidate(
        "SELECT id, amount FROM 'in.vortex' WHERE amount > 0 LIMIT 5"
    ));
}

#[test]
fn binds_window_over_grouped_aggregate_named_window_and_exclude_frame() {
    let plan = bind_sql(
        "SELECT grp, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rk, SUM(SUM(amount)) OVER w AS running FROM 'sales.vortex' GROUP BY grp WINDOW w AS (ORDER BY SUM(amount) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)",
    )
    .expect("window over aggregate + named window + exclude binds");

    let LogicalRelation::Project { input, .. } = &plan.root else {
        panic!("expected Project root");
    };
    let LogicalRelation::Window {
        input: win_in,
        windows,
    } = input.as_ref()
    else {
        panic!("expected Window wrapping Aggregate");
    };
    assert!(
        matches!(win_in.as_ref(), LogicalRelation::Aggregate { aggregates, .. } if !aggregates.is_empty())
    );
    assert_eq!(windows.len(), 2);
    assert_eq!(
        windows[1]
            .1
            .frame
            .as_ref()
            .and_then(|f| f.exclusion.as_deref()),
        Some("CURRENT ROW")
    );
}

#[test]
fn handles_qualify_fetch_quantified_candidate_and_null_safe_is_truth() {
    assert!(is_relational_sql_candidate(
        "SELECT id FROM 'in.csv' WHERE amount > ALL (SELECT threshold FROM 't.csv')"
    ));
    assert!(is_relational_sql_candidate(
        "SELECT id FROM 'in.csv' WHERE amount = ANY (SELECT threshold FROM 't.csv')"
    ));

    let qualify_err = bind_sql(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM 'in.parquet' QUALIFY rn = 1",
    )
    .expect_err("QUALIFY must fail explicitly");
    assert!(qualify_err.to_string().contains("QUALIFY"), "{qualify_err}");

    let fetch_plan =
        bind_sql("SELECT id FROM 'in.parquet' ORDER BY id OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY")
            .expect("FETCH FIRST/NEXT should lower to Limit");
    assert!(matches!(
        &fetch_plan.root,
        LogicalRelation::Limit {
            limit: Some(LogicalExpr::Literal(ScalarValue::Int64(3))),
            offset: Some(LogicalExpr::Literal(ScalarValue::Int64(2))),
            ..
        }
    ));
    for bad_fetch in [
        "SELECT id FROM 'in.parquet' ORDER BY id FETCH FIRST 1 ROW WITH TIES",
        "SELECT id FROM 'in.parquet' ORDER BY id FETCH FIRST 10 PERCENT ROWS ONLY",
        "SELECT id FROM 'in.parquet' ORDER BY id LIMIT 1 FETCH FIRST 2 ROWS ONLY",
    ] {
        assert!(bind_sql(bad_fetch).is_err(), "{bad_fetch}");
    }

    let truth_plan = bind_sql("SELECT flag IS TRUE AS t, flag IS FALSE AS f FROM 'in.parquet'")
        .expect("IS TRUE / IS FALSE must bind null-safely");
    let LogicalRelation::Project { items, .. } = &truth_plan.root else {
        panic!("expected Project");
    };
    assert!(matches!(
        &items[0],
        LogicalProjectionItem::Expr {
            expr,
            alias: Some(a),
        } if a == "t"
            && matches!(
                expr.as_ref(),
                LogicalExpr::IsDistinctFrom {
                    right,
                    negated: true,
                    ..
                } if matches!(right.as_ref(), LogicalExpr::Literal(ScalarValue::Boolean(true)))
            )
    ));
    assert!(matches!(
        &items[1],
        LogicalProjectionItem::Expr {
            expr,
            alias: Some(a),
        } if a == "f"
            && matches!(
                expr.as_ref(),
                LogicalExpr::IsDistinctFrom {
                    right,
                    negated: true,
                    ..
                } if matches!(right.as_ref(), LogicalExpr::Literal(ScalarValue::Boolean(false)))
            )
    ));
}

#[test]
fn rejects_wildcard_modifiers_and_expr_qualified_wildcards() {
    for sql in [
        "SELECT * EXCLUDE (secret), ROW_NUMBER() OVER (ORDER BY id) AS rn FROM 'in.parquet'",
        "SELECT * REPLACE (id + 1 AS id), ROW_NUMBER() OVER (ORDER BY id) AS rn FROM 'in.parquet'",
        "SELECT t.* EXCLUDE (secret) FROM 'in.parquet' AS t",
        "SELECT COUNT(* EXCLUDE (secret)) FROM 'in.parquet'",
    ] {
        let err = bind_sql(sql).expect_err(sql);
        assert!(err.to_string().contains("wildcard"), "{sql}: {err}");
    }
    for sql in [
        "SELECT t.* EXCEPT (secret) FROM 'in.parquet' AS t",
        "SELECT * RENAME (id AS ident) FROM 'in.parquet'",
    ] {
        assert!(bind_sql(sql).is_err(), "{sql}");
    }
}

#[test]
fn rejects_by_name_sets_ignore_nulls_windows_and_silent_ast_modifiers() {
    for sql in [
        "SELECT a, b FROM 'left.parquet' UNION ALL BY NAME SELECT b, a FROM 'right.parquet'",
        "SELECT a, b FROM 'left.parquet' UNION BY NAME SELECT b, a FROM 'right.parquet'",
        "SELECT a, b FROM 'left.parquet' UNION DISTINCT BY NAME SELECT b, a FROM 'right.parquet'",
        "SELECT FIRST_VALUE(v) IGNORE NULLS OVER (ORDER BY id) FROM 'in.parquet'",
        "SELECT FIRST_VALUE(v IGNORE NULLS) OVER (ORDER BY id) FROM 'in.parquet'",
        "SELECT COUNT(v RESPECT NULLS) FROM 'in.parquet'",
        "SELECT * FROM 'in.parquet' TABLESAMPLE BERNOULLI (10)",
        "SELECT * FROM (SELECT id FROM 'in.parquet') AS t(renamed_id)",
        "SELECT SUBSTR(str => name, pos => 1) FROM 'in.parquet'",
    ] {
        assert!(bind_sql(sql).is_err(), "{sql}");
    }
    assert!(
        bind_sql("SELECT FIRST_VALUE(v) RESPECT NULLS OVER (ORDER BY id) FROM 'in.parquet'")
            .is_ok()
    );
}

#[test]
fn resolves_named_windows_in_query_order_by_and_rejects_unknown_or_conflicting_refs() {
    let plan = bind_sql(
        "SELECT id FROM 'in.parquet' WINDOW w AS (ORDER BY id DESC) ORDER BY ROW_NUMBER() OVER w LIMIT 1",
    )
    .expect("named window in ORDER BY binds");

    let LogicalRelation::Limit {
        input: sort_rel, ..
    } = &plan.root
    else {
        panic!("expected Limit root");
    };
    let LogicalRelation::Sort {
        input: proj_rel,
        keys,
    } = sort_rel.as_ref()
    else {
        panic!("expected Sort under Limit");
    };
    let LogicalRelation::Project { input: win_rel, .. } = proj_rel.as_ref() else {
        panic!("expected Project under Sort");
    };
    let LogicalRelation::Window { windows, .. } = win_rel.as_ref() else {
        panic!("expected Window under Project");
    };
    assert_eq!(windows.len(), 1);
    assert!(windows[0].1.name.is_none());
    assert_eq!(windows[0].1.order_by.len(), 1);
    let LogicalExpr::Function {
        over: Some(spec), ..
    } = &keys[0].expr
    else {
        panic!("expected window function in ORDER BY key");
    };
    assert!(spec.name.is_none());
    assert_eq!(spec.order_by.len(), 1);

    assert!(
        bind_sql(
            "SELECT id, SUM(v) OVER w2 FROM 'in.parquet' WINDOW w1 AS (PARTITION BY grp), w2 AS (w1 ORDER BY id)"
        )
        .is_ok()
    );

    for bad_sql in [
        "SELECT ROW_NUMBER() OVER missing_w FROM 'in.parquet'",
        "SELECT id FROM 'in.parquet' ORDER BY ROW_NUMBER() OVER missing_w",
        "SELECT id FROM 'in.parquet' WINDOW w AS (ORDER BY id) ORDER BY ROW_NUMBER() OVER (w ORDER BY id DESC)",
        "SELECT id FROM 'in.parquet' WINDOW w AS (ORDER BY id), w AS (ORDER BY id DESC) ORDER BY ROW_NUMBER() OVER w",
    ] {
        assert!(bind_sql(bad_sql).is_err(), "{bad_sql}");
    }
}
