//! Rust callbacks keep declared schemas and placement through SQL composition.

use super::*;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use shardloom_core::expression::{Determinism, EffectLevel, NullBehavior};
use shardloom_vortex::application_batch::{
    ApplicationBatchDeclaration, ApplicationBatchFunction, ApplicationBatchLimits,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn application_rust_declaration_schema_survives_sql_materialized_join_and_repeated_actions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let fields = |name: &str| {
        ApplicationBatchDeclaration::parse_fields(&[(
            name.into(),
            r#"{"Primitive":["i64",false]}"#.into(),
        )])
        .unwrap()
    };
    let function = ApplicationBatchFunction::new_trusted(
        ApplicationBatchDeclaration {
            name: "add_three".into(),
            version: "1".into(),
            input: fields("value"),
            output: fields("shifted"),
            determinism: Determinism::Deterministic,
            null_behavior: NullBehavior::NullAware,
            effect: EffectLevel::PureDeterministic,
            limits: ApplicationBatchLimits {
                max_batch_rows: 3,
                max_output_bytes: 4096,
                workspace_bytes: 4096,
            },
        },
        move |_, invocation| {
            invocation.check_cancelled()?;
            let batch = invocation.to_arrow()?;
            observed.fetch_add(batch.num_rows(), Ordering::Relaxed);
            let values = batch.column(0).as_primitive::<Int64Type>();
            let output = Int64Array::from_iter_values(values.values().iter().map(|v| v + 3));
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "shifted",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(output)],
            )
            .unwrap()
            .into())
        },
    )
    .unwrap();
    let input = Arc::new(NativeRelationDeclaration::scan("memory://input").unwrap());
    let mapped = Arc::new(input.map_batches(function).unwrap());
    let declaration = NativeRelationDeclaration::sql(
        "WITH mapped AS MATERIALIZED (SELECT * FROM transformed) SELECT a.shifted AS value, b.shifted AS copy FROM mapped AS a INNER JOIN mapped AS b ON a.shifted=b.shifted WHERE a.shifted>=8 ORDER BY a.shifted",
        vec![("transformed".into(), mapped)],
    )
    .unwrap();
    let session = session();
    let memory = session.shared_memory_pool();
    let source = ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 8).unwrap();
    let inputs = [binding(source)];
    let (operation, _) = declaration
        .prepare(
            allocation(),
            &session,
            &inputs,
            None,
            &CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let baseline = memory.snapshot().reserved_bytes;
    for action in 1..=2 {
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let rows: Vec<Value> = result
            .result_jsonl
            .value()
            .lines()
            .map(|row| serde_json::from_str(row).unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                json!({"value":8,"copy":8}),
                json!({"value":9,"copy":9}),
                json!({"value":10,"copy":10})
            ]
        );
        assert_eq!(calls.load(Ordering::Relaxed), action * 8);
        assert_eq!(result.execution.composition.materialized_relations, 1);
        assert_eq!(result.execution.composition.materialized_reuses, 1);
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    drop((operation, inputs, session, declaration));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
