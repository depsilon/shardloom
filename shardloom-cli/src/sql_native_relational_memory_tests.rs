use super::*;
use shardloom_vortex::{
    resident_memory_source::{MemoryColumn, MemoryColumnValues, ResidentMemorySource},
    resident_session::ResidentVortexSession,
};

fn batch_source(
    session: &ResidentVortexSession,
    values: &[Option<i64>],
) -> NativeResult<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[MemoryColumn {
            name: "n",
            values: MemoryColumnValues::Int64(values),
        }],
    )
}

#[test]
fn native_relational_sql_streaming_input_preserves_synthetic_limit_origin_and_end() {
    let uri = DatasetUri::new("memory://stream").unwrap();
    for statement in [
        "SELECT n FROM 'memory://stream' WHERE n > 0",
        "SELECT n FROM (SELECT n FROM 'memory://stream' WHERE n > 0) AS s",
        "WITH s AS (SELECT n FROM 'memory://stream' WHERE n > 0) SELECT n FROM s",
    ] {
        let prepared = prepare_with_inputs(
            statement,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |schemas| {
                schemas.register_batch_source(uri.clone(), |session| batch_source(session, &[]))
            },
            |_| Ok(vec![uri.clone()]),
        )
        .unwrap_or_else(|error| panic!("{statement}: {error}"));
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 => batch_source(session, &[Some(1), None]).map(Some),
                2 => batch_source(session, &[Some(-1), Some(2)]).map(Some),
                3 => Ok(None),
                _ => panic!("source replayed"),
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, 3);
        assert_eq!(result.result_jsonl.value(), "{\"n\":1}\n{\"n\":2}\n");
        let report = result.execution.input.unwrap();
        assert_eq!(report.rows, 4);
        assert!(report.end_of_input_observed);
    }
}

#[test]
fn native_relational_sql_streaming_explicit_limits_drain_and_preserve_late_failures() {
    let uri = DatasetUri::new("memory://stream").unwrap();
    for (limit, expected) in [
        (0, ""),
        (1, "{\"n\":1}\n"),
        (
            usize::MAX,
            "{\"n\":1}\n{\"n\":null}\n{\"n\":-1}\n{\"n\":2}\n",
        ),
    ] {
        let statement = format!("SELECT n FROM 'memory://stream' LIMIT {limit}");
        let prepared = prepare_with_inputs(
            &statement,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |schemas| {
                schemas.register_batch_source(uri.clone(), |session| batch_source(session, &[]))
            },
            |_| Ok(vec![uri.clone()]),
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 => batch_source(session, &[Some(1), None]).map(Some),
                2 => batch_source(session, &[Some(-1), Some(2)]).map(Some),
                3 => Ok(None),
                _ => panic!("source replayed"),
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(result.result_jsonl.value(), expected);
        assert_eq!(calls, 3, "even LIMIT 0 must drain through end-of-input");
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(report.rows, 4);
        assert!(report.end_of_input_observed);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);

        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 1 {
                batch_source(session, &[Some(1)]).map(Some)
            } else {
                Err(unsupported_sql_error("late streaming limit producer"))
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("late streaming limit producer")
        );
        assert_eq!(calls, 2);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

pub(super) fn verify_memory(statement: &str, expected: &Value) {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let prepared = prepare(statement, policy, |_| {
        panic!("memory query must not resolve a file")
    })
    .unwrap_or_else(|error| panic!("{statement}: {error}"));
    assert_eq!(prepared.snapshot().prepared_source_opens, 0);
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
        let rows = result
            .result_jsonl
            .value()
            .lines()
            .map(|row| serde_json::from_str::<Value>(row).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(&json!(rows), expected, "{statement}");
        assert_eq!(result.execution.runtime.prepared_source_opens, 0);
        assert_eq!(result.execution.runtime.completed_executions, execution);
    }
}

#[test]
fn native_relational_sql_memory_literals_and_values_share_native_expressions() {
    verify_memory(
        "SELECT 9223372036854775807 AS n, 'λ,;''%=' AS s, TRUE AS b, NULL AS absent",
        &json!([{"n":i64::MAX,"s":"λ,;'%=","b":true,"absent":null}]),
    );
    verify_memory("SELECT 1 + 2 AS result WHERE FALSE", &json!([]));
    verify_memory("SELECT 1 + 2 AS result", &json!([{"result":3}]));
    verify_memory("SELECT 7, 'a'", &json!([{"column_1":7,"column_2":"a"}]));
    verify_memory(
        "VALUES (1, 'a,;''%'), (2, 'λ')",
        &json!([{"column_1":1,"column_2":"a,;'%"},{"column_1":2,"column_2":"λ"}]),
    );
    verify_memory(
        "SELECT column_1 * 2 AS doubled FROM (VALUES (1), (3), (2)) AS v WHERE column_1 > 1 ORDER BY doubled DESC",
        &json!([{"doubled":6},{"doubled":4}]),
    );
    verify_memory(
        "VALUES (1.5), (2)",
        &json!([{"column_1":1.5},{"column_1":2.0}]),
    );
    verify_memory(
        "VALUES (NULL, 'null'), (9223372036854775807, NULL), (-9223372036854775808, '')",
        &json!([{"column_1":null,"column_2":"null"},{"column_1":i64::MAX,"column_2":null},{"column_1":i64::MIN,"column_2":""}]),
    );
    verify_memory(
        "VALUES (NULL), (NULL)",
        &json!([{"column_1":null},{"column_1":null}]),
    );
    verify_memory(
        "SELECT COUNT(column_1) AS n,SUM(column_1) AS total FROM (VALUES (NULL), (1), (2)) AS v",
        &json!([{"n":2,"total":3.0}]),
    );
}

#[test]
fn native_relational_sql_memory_ranges_compose_with_existing_operators() {
    verify_memory(
        "SELECT value * 2 AS doubled FROM range(1, 5) WHERE value > 1 ORDER BY doubled DESC LIMIT 2",
        &json!([{"doubled":8},{"doubled":6}]),
    );
    verify_memory(
        "SELECT value FROM generate_series(5, 1, -2) ORDER BY value",
        &json!([{"value":1},{"value":3},{"value":5}]),
    );
    verify_memory(
        "SELECT COUNT(*) AS n, SUM(value) AS total FROM range(1, 5)",
        &json!([{"n":4,"total":10.0}]),
    );
    verify_memory("SELECT COUNT(*) AS n FROM range(5, 1)", &json!([{"n":0}]));
    verify_memory(
        "SELECT l.value AS value FROM range(1, 4) AS l JOIN generate_series(2, 4) AS r ON l.value = r.value ORDER BY value",
        &json!([{"value":2},{"value":3}]),
    );
    verify_memory(
        "SELECT value FROM range(1, 4) WHERE value IN (SELECT value FROM range(2, 5)) ORDER BY value",
        &json!([{"value":2},{"value":3}]),
    );
    verify_memory(
        "SELECT value FROM range(1, 3) UNION SELECT value FROM range(2, 4) ORDER BY value",
        &json!([{"value":1},{"value":2},{"value":3}]),
    );
    verify_memory(
        "SELECT value,ROW_NUMBER() OVER (ORDER BY value DESC) AS position FROM range(1, 4) ORDER BY position LIMIT 2",
        &json!([{"value":3,"position":1},{"value":2,"position":2}]),
    );
    verify_memory(
        "SELECT * FROM PIVOT((SELECT value AS entity,'a' AS category,value * 10 AS amount FROM range(1, 3)), '{\"index\":\"entity\",\"columns\":\"category\",\"values\":\"amount\",\"aggregate\":\"sum\"}') AS p",
        &json!([{"entity":1,"pivot_a":10.0},{"entity":2,"pivot_a":20.0}]),
    );
}

#[test]
fn native_relational_sql_memory_rejects_invalid_declarations_before_source_access() {
    for statement in [
        "SELECT unknown",
        "SELECT __shardloom_unit",
        "SELECT *",
        "SELECT value FROM range(1, 5, 0)",
        "SELECT value FROM range(0, 1000001)",
        "VALUES (1), (2, 3)",
        "VALUES (1), ('a')",
        "VALUES (9007199254740993), (1.5)",
        "VALUES (1 + 2)",
    ] {
        assert!(
            prepare(
                statement,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                |_| panic!("invalid declaration must not resolve a file")
            )
            .is_err(),
            "{statement}"
        );
    }
}
