//! SQL scopes reach the same native action-scoped retention and iteration owners.

use super::*;
use shardloom_core::{FloatPolicy, RecursionLimit};
use shardloom_vortex::local_primitives::prepared_relational::CollectedVortexRelational;

fn prepared(query: &str, limit: u64) -> NativeResult<PreparedVortexRelational> {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)?;
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let session = ResidentVortexSession::new_with_policies(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
        FloatPolicy::Strict,
        RecursionLimit::new(limit)?,
    )?;
    prepare_in_session(
        query,
        policy,
        &session,
        |_| Ok(()),
        |_, _| panic!("retained SQL fixtures must not access external sources"),
        &CancellationToken::default(),
    )
}

fn rows(result: &CollectedVortexRelational) -> Value {
    json!(
        result
            .result_jsonl
            .value()
            .lines()
            .map(|row| { serde_json::from_str::<Value>(row).unwrap() })
            .collect::<Vec<_>>()
    )
}

#[test]
fn retained_sql_recursive_columns_qualifiers_and_limit_preserve_complete_iterations() {
    let query = "WITH RECURSIVE numbers(n) AS (SELECT 0 AS original UNION ALL SELECT r.n+1 AS next FROM numbers AS r WHERE r.n<37) SELECT n FROM numbers ORDER BY n";
    let plan = prepared(query, 38).unwrap();
    let baseline = plan.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let result = plan.collect_jsonl(&CancellationToken::default()).unwrap();
        assert_eq!(
            rows(&result),
            json!((0..=37).map(|n| json!({"n":n})).collect::<Vec<_>>())
        );
        assert_eq!(result.execution.composition.recursive_relations, 1);
        assert_eq!(result.execution.composition.recursive_iterations, 38);
        drop(result);
        assert_eq!(plan.snapshot().memory.reserved_bytes, baseline);
    }
    for limit in [0, 1] {
        let plan = prepared(&format!("{query} LIMIT {limit}"), 37).unwrap();
        let error = plan
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("SL_RECURSION_LIMIT_EXCEEDED"),
            "{error}"
        );
    }
}

#[test]
fn retained_sql_materialized_cte_reuses_complete_rows_but_not_across_actions() {
    let query = "WITH values_cte(x) AS MATERIALIZED (SELECT value FROM range(0,5)) SELECT a.x AS left_value,b.x AS right_value FROM values_cte AS a JOIN values_cte AS b ON a.x=b.x WHERE a.x>1 ORDER BY a.x LIMIT 2";
    let plan = prepared(query, 10).unwrap();
    for _ in 0..2 {
        let result = plan.collect_jsonl(&CancellationToken::default()).unwrap();
        assert_eq!(
            rows(&result),
            json!([
                {"left_value":2,"right_value":2},
                {"left_value":3,"right_value":3}
            ])
        );
        assert_eq!(result.execution.composition.materialized_relations, 1);
        assert_eq!(result.execution.composition.materialized_rows, 5);
        assert_eq!(result.execution.composition.materialized_reuses, 1);
        assert_eq!(result.execution.scan_rows_delivered, 5);
    }
    for modifier in ["", "NOT MATERIALIZED "] {
        let query = format!(
            "WITH value_cte AS {modifier}(SELECT value FROM range(0,2)) SELECT value FROM value_cte UNION ALL SELECT value FROM value_cte ORDER BY value"
        );
        let result = prepared(&query, 10)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            rows(&result),
            json!([{"value":0},{"value":0},{"value":1},{"value":1}])
        );
        assert_eq!(result.execution.composition.materialized_relations, 0);
    }
}

#[test]
fn retained_sql_recursive_graph_joins_materialized_edges_and_stops_duplicate_cycles() {
    let query = "WITH RECURSIVE edges(src,dst) AS MATERIALIZED (SELECT column_1,column_2 FROM (VALUES (1,2),(1,3),(2,4),(3,4),(4,1)) AS edge_values), reached(node) AS (SELECT 1 AS initial UNION SELECT e.dst FROM reached AS r JOIN edges AS e ON r.node=e.src) SELECT a.node AS node,b.node AS repeated FROM reached AS a JOIN reached AS b ON a.node=b.node ORDER BY a.node";
    let result = prepared(query, 4)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        rows(&result),
        json!([
            {"node":1,"repeated":1}, {"node":2,"repeated":2},
            {"node":3,"repeated":3}, {"node":4,"repeated":4}
        ])
    );
    let report = result.execution.composition;
    assert_eq!(report.recursive_relations, 1);
    assert_eq!(report.recursive_reuses, 1);
    assert_eq!(report.recursive_iterations, 3);
    assert_eq!(report.materialized_relations, 1);
    assert_eq!(report.materialized_rows, 5);
    assert_eq!(report.materialized_reuses, 2);
}

#[test]
fn retained_sql_empty_anchors_unreferenced_ctes_and_nested_lexical_scopes_are_inert() {
    let query = "WITH RECURSIVE unused AS MATERIALIZED (SELECT missing FROM 'absent.vortex'), empty(n) AS (SELECT value FROM range(0,0) UNION ALL SELECT n+1 FROM empty) SELECT n FROM empty";
    let result = prepared(query, 1)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(rows(&result), json!([]));
    assert_eq!(result.execution.composition.recursive_iterations, 0);
    assert_eq!(result.execution.composition.materialized_relations, 0);
    let query = "WITH RECURSIVE n(x) AS (SELECT 1 AS x UNION ALL SELECT x+1 FROM n WHERE x<2), next_cte(y) AS MATERIALIZED (SELECT x FROM n), n2(x) AS (SELECT y FROM next_cte UNION SELECT x FROM n2) SELECT x FROM n2 ORDER BY x";
    let result = prepared(query, 2)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(rows(&result), json!([{"x":1},{"x":2}]));
    assert_eq!(result.execution.composition.recursive_relations, 2);
    assert_eq!(result.execution.composition.materialized_relations, 1);
}

#[test]
fn retained_sql_rejects_invalid_recursive_scopes_and_domain_changes() {
    for query in [
        "WITH x AS (SELECT n FROM x) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT n FROM y UNION ALL SELECT n FROM x), y(n) AS (SELECT n FROM x) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT n FROM x UNION ALL SELECT n FROM x) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT 1 AS n EXCEPT SELECT n FROM x) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT 1 AS n UNION ALL SELECT a.n FROM x AS a JOIN x AS b ON a.n=b.n) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT 1 AS n UNION ALL SELECT 'changed' AS n FROM x) SELECT n FROM x",
        "WITH RECURSIVE x(n) AS (SELECT 1 AS n UNION ALL SELECT COUNT(*) AS n FROM x) SELECT n FROM x",
    ] {
        assert!(prepared(query, 10).is_err(), "{query}");
    }
}

#[test]
fn retained_sql_materialization_does_not_hide_failing_unselected_tail_values() {
    for limit in [0, 1] {
        let query = format!(
            "WITH retained AS MATERIALIZED (SELECT 10/(4-value) AS n FROM range(0,5)) SELECT n FROM retained LIMIT {limit}"
        );
        let plan = prepared(&query, 10).unwrap();
        let baseline = plan.snapshot().memory.reserved_bytes;
        let mut delivered = 0;
        let error = plan
            .for_each_json_batch(&CancellationToken::default(), 1, 1024, |_| {
                delivered += 1;
                Ok(())
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("division by zero"), "{error}");
        assert_eq!(delivered, 0);
        assert_eq!(plan.snapshot().memory.reserved_bytes, baseline);
    }
}
