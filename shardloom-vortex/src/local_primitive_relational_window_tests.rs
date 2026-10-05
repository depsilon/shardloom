use super::*;
use crate::relational_query::{
    VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey as OrderKey,
    VortexRelationalWindow as Window, VortexRelationalWindowExpression as Expression,
    VortexRelationalWindowFunction as Function,
};
use vortex::array::arrays::{DictArray, VarBinViewArray};

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

fn expression(
    name: &str,
    function: Function,
    descending: bool,
    nulls: Option<NullOrder>,
) -> Expression {
    Expression {
        output_column: name.into(),
        function,
        partition_by: vec![column("group")],
        order_by: vec![OrderKey {
            column: column("priority"),
            descending,
            nulls,
        }],
        frame: None,
    }
}

pub(super) fn plan(input: VortexRelationalPlan) -> VortexRelationalPlan {
    let mut expressions = [
        ("rn", Function::RowNumber),
        ("rank", Function::Rank),
        ("dense", Function::DenseRank),
        (
            "lag",
            Function::Lag {
                column: column("value"),
                offset: 1,
            },
        ),
        (
            "lead",
            Function::Lead {
                column: column("value"),
                offset: 2,
            },
        ),
        ("tile", Function::Ntile { buckets: 3 }),
        ("percent", Function::PercentRank),
        ("cume", Function::CumeDist),
    ]
    .into_iter()
    .map(|(name, function)| expression(name, function, false, Some(NullOrder::Last)))
    .collect::<Vec<_>>();
    expressions.push(expression(
        "reverse",
        Function::RowNumber,
        true,
        Some(NullOrder::First),
    ));
    VortexRelationalPlan::Window(Box::new(Window {
        input,
        columns: vec![column("value")],
        expressions,
    }))
}

pub(super) fn fixture() -> Fixture {
    let groups = DictArray::try_new(
        PrimitiveArray::from_iter([0u8, 1, 1, 1, 0, 0, 2, 2, 1]).into_array(),
        VarBinViewArray::from_iter_nullable_str([Some("B"), Some("東京"), None]).into_array(),
    )
    .unwrap()
    .into_array();
    Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "priority", "value"]),
            vec![
                groups,
                PrimitiveArray::from_option_iter([
                    Some(3i16),
                    Some(2),
                    Some(1),
                    Some(1),
                    Some(1),
                    None,
                    Some(2),
                    Some(2),
                    None,
                ])
                .into_array(),
                PrimitiveArray::from_iter([30u32, 20, 10, 11, 40, 50, 60, 61, 90]).into_array(),
            ],
            9,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

pub(super) fn expected() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"value":30,"rn":2,"rank":2,"dense":2,"lag":40,"lead":null,"tile":2,"percent":0.5,"cume":2.0/3.0,"reverse":2}),
        serde_json::json!({"value":20,"rn":3,"rank":3,"dense":2,"lag":11,"lead":null,"tile":2,"percent":2.0/3.0,"cume":0.75,"reverse":2}),
        serde_json::json!({"value":10,"rn":1,"rank":1,"dense":1,"lag":null,"lead":20,"tile":1,"percent":0.0,"cume":0.5,"reverse":3}),
        serde_json::json!({"value":11,"rn":2,"rank":1,"dense":1,"lag":10,"lead":90,"tile":1,"percent":0.0,"cume":0.5,"reverse":4}),
        serde_json::json!({"value":40,"rn":1,"rank":1,"dense":1,"lag":null,"lead":50,"tile":1,"percent":0.0,"cume":1.0/3.0,"reverse":3}),
        serde_json::json!({"value":50,"rn":3,"rank":3,"dense":3,"lag":30,"lead":null,"tile":3,"percent":1.0,"cume":1.0,"reverse":1}),
        serde_json::json!({"value":60,"rn":1,"rank":1,"dense":1,"lag":null,"lead":null,"tile":1,"percent":0.0,"cume":1.0,"reverse":1}),
        serde_json::json!({"value":61,"rn":2,"rank":1,"dense":1,"lag":60,"lead":null,"tile":2,"percent":0.0,"cume":1.0,"reverse":2}),
        serde_json::json!({"value":90,"rn":4,"rank":4,"dense":3,"lag":20,"lead":null,"tile":3,"percent":1.0,"cume":1.0,"reverse":1}),
    ]
}

#[test]
fn native_relational_windows_cover_all_functions_ties_null_partitions_and_source_order() {
    let fixture = fixture();
    let prepared = prepare_relational(&plan(fixture.scan()), policy()).unwrap();
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("static plan")
    };
    let NodeKind::Window { spec, .. } = &root.kind else {
        panic!("expected window")
    };
    assert_eq!(
        spec.groups.len(),
        2,
        "matching partition/order specs share one sort"
    );
    for call in 1..=2 {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&collected), expected());
        assert_eq!(collected.execution.runtime.prepared_source_opens, 1);
        assert_eq!(collected.execution.runtime.completed_executions, call);
    }
    assert_eq!(
        root.fields[0].1,
        DType::Primitive(PType::U32, Nullability::NonNullable)
    );
    assert_eq!(
        root.fields[4].1,
        DType::Primitive(PType::U32, Nullability::Nullable)
    );
}

pub(super) fn unpartitioned(fixture: &Fixture, function: Function) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![column("value")],
        expressions: vec![Expression {
            output_column: "window".into(),
            function,
            partition_by: vec![],
            order_by: vec![OrderKey {
                column: column("value"),
                descending: false,
                nulls: None,
            }],
            frame: None,
        }],
    }))
}

#[test]
fn native_relational_windows_preserve_u64_key_order_signed_zero_and_empty_schema() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([u64::MAX, 0, u64::MAX - 1]).into_array(),
        ),
        1,
    );
    let prepared =
        prepare_relational(&unpartitioned(&fixture, Function::RowNumber), policy()).unwrap();
    let rows = json_rows(
        &prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"value":u64::MAX,"window":3}),
            serde_json::json!({"value":0,"window":1}),
            serde_json::json!({"value":u64::MAX-1,"window":2}),
        ]
    );
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([-0.0f64, 0.0, 1.0]).into_array(),
        ),
        1,
    );
    let prepared = prepare_relational(&unpartitioned(&fixture, Function::Rank), policy()).unwrap();
    let rows = json_rows(
        &prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert_eq!(
        rows.iter()
            .map(|row| row["window"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [1, 1, 3]
    );
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter(Vec::<u16>::new()).into_array(),
        ),
        1,
    );
    let prepared = prepare_relational(
        &unpartitioned(
            &fixture,
            Function::Lag {
                column: column("value"),
                offset: 100,
            },
        ),
        policy(),
    )
    .unwrap();
    let result = prepared.execute_owned().unwrap();
    assert_eq!(result.execution.output_rows, 0);
    assert_eq!(result.execution.output_batches, 1);
    assert_eq!(
        result.result.dtype(),
        &DType::struct_(
            [
                (
                    "value",
                    DType::Primitive(PType::U16, Nullability::NonNullable)
                ),
                (
                    "window",
                    DType::Primitive(PType::U16, Nullability::Nullable)
                ),
            ],
            Nullability::NonNullable
        )
    );
}

#[test]
fn native_relational_windows_reject_invalid_parameters_nonfinite_and_implicit_null_order() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter([1u32]).into_array()),
        1,
    );
    for function in [
        Function::Ntile { buckets: 0 },
        Function::Lag {
            column: column("value"),
            offset: 0,
        },
    ] {
        assert!(prepare_relational(&unpartitioned(&fixture, function), policy()).is_err());
    }
    for array in [
        PrimitiveArray::from_option_iter([None, Some(1u32)]).into_array(),
        PrimitiveArray::from_iter([f64::NAN]).into_array(),
    ] {
        let fixture = Fixture::new(single("value", array), 1);
        let prepared =
            prepare_relational(&unpartitioned(&fixture, Function::RowNumber), policy()).unwrap();
        let baseline = prepared.session.memory().snapshot().reserved_bytes;
        let mut delivered = 0;
        assert!(
            prepared
                .for_each_batch(&CancellationToken::default(), |_, _| {
                    delivered += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(delivered, 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
    }
}

#[test]
fn native_relational_window_cancellation_after_one_batch_releases_partition_state_and_does_not_succeed()
 {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter((0u32..5000).rev()).into_array(),
        ),
        1024,
    );
    let prepared =
        prepare_relational(&unpartitioned(&fixture, Function::RowNumber), policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let cancel = CancellationToken::default();
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&cancel, |array, _| {
                assert_eq!(array.len(), BATCH_ROWS);
                delivered += 1;
                cancel.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
}

#[test]
fn native_relational_window_denial_releases_all_state_and_a_fresh_call_retries_without_reopening() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter(0u32..8192).into_array()),
        1024,
    );
    let prepared =
        prepare_relational(&unpartitioned(&fixture, Function::RowNumber), policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let limit = prepared.session.memory().snapshot().limit_bytes;
    let pressure = prepared
        .session
        .memory()
        .reserve(limit - baseline - 128 * 1024)
        .unwrap();
    let mut delivered = 0;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            delivered += 1;
            Ok(())
        })
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("budget") || error.to_string().contains("reservation"),
        "{error}"
    );
    assert_eq!(delivered, 0);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(pressure);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    let result = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
        .unwrap();
    assert_eq!(result.output_rows, 8192);
    assert_eq!(result.runtime.prepared_source_opens, 1);
    assert_eq!(result.runtime.completed_executions, 1);
    drop(result);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
}

#[test]
fn native_relational_window_singleton_distribution_and_offsets_above_partition_size_are_exact() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter([7u8]).into_array()),
        1,
    );
    for (function, expected) in [
        (
            Function::Ntile {
                buckets: usize::MAX,
            },
            serde_json::json!(1),
        ),
        (Function::PercentRank, serde_json::json!(0.0)),
        (Function::CumeDist, serde_json::json!(1.0)),
        (
            Function::Lead {
                column: column("value"),
                offset: usize::MAX,
            },
            serde_json::Value::Null,
        ),
        (
            Function::Lag {
                column: column("value"),
                offset: usize::MAX,
            },
            serde_json::Value::Null,
        ),
    ] {
        let prepared = prepare_relational(&unpartitioned(&fixture, function), policy()).unwrap();
        assert_eq!(
            json_rows(
                &prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            )[0]["window"],
            expected
        );
    }
}
