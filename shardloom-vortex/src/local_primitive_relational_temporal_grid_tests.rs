//! Typed grid expressions reuse native projection, ownership and admission.

use super::*;

fn gap_fill(
    input: VortexRelationalPlan,
    by: &[&str],
    start: Option<i64>,
    end: Option<i64>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::GapFill(Box::new(crate::relational_query::VortexRelationalGapFill {
        input,
        time: ColumnRef::new("instant").unwrap(),
        partition_by: by
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
        step: shardloom_core::temporal::TemporalGridStep::Duration {
            value: 2,
            unit: shardloom_core::temporal::TemporalUnit::Seconds,
        },
        origin: None,
        start,
        end,
    }))
}

#[test]
fn native_temporal_gap_fill_sorts_partitions_and_streams_exact_missing_rows() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["fund", "instant", "value"]),
            vec![
                VarBinArray::from(vec![Some("b"), Some("a"), Some("a"), Some("b"), None])
                    .into_array(),
                temporal_array(
                    "timestamp(s)",
                    &[Some(4), Some(2), Some(0), Some(0), Some(2)],
                ),
                PrimitiveArray::from_option_iter([Some(40i64), None, Some(7), Some(20), Some(10)])
                    .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let plan = gap_fill(fixture.scan(), &["fund"], Some(0), Some(4));
    assert_eq!(
        collect(&plan),
        vec![
            json!({"fund":null,"instant":0,"value":null}),
            json!({"fund":null,"instant":2,"value":10}),
            json!({"fund":null,"instant":4,"value":null}),
            json!({"fund":"a","instant":0,"value":7}),
            json!({"fund":"a","instant":2,"value":null}),
            json!({"fund":"a","instant":4,"value":null}),
            json!({"fund":"b","instant":0,"value":20}),
            json!({"fund":"b","instant":2,"value":null}),
            json!({"fund":"b","instant":4,"value":40}),
        ]
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let expected = prepared.output_dtype().unwrap();
    assert!(
        !expected
            .as_struct_fields_opt()
            .unwrap()
            .field_by_index(1)
            .unwrap()
            .is_nullable()
    );
    let memory = prepared.session.memory().clone();
    let output = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    drop((prepared, fixture));
    assert_eq!(output.result.dtype(), &expected);
    assert_eq!(output.result.row_count(), 9);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_temporal_gap_fill_empty_bounds_validation_and_bounded_consumer_failure() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["instant", "value"]),
            vec![
                temporal_array("timestamp(s)", &[Some(0), Some(4)]),
                PrimitiveArray::from_iter([1i64, 9]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    assert_eq!(
        collect(&gap_fill(empty(fixture.scan()), &[], Some(-2), Some(2))),
        vec![
            json!({"instant":-2,"value":null}),
            json!({"instant":0,"value":null}),
            json!({"instant":2,"value":null}),
        ]
    );
    assert_eq!(
        collect(&gap_fill(empty(fixture.scan()), &[], None, Some(2))),
        [] as [serde_json::Value; 0]
    );
    for (start, end) in [(Some(4), Some(2)), (Some(1), Some(2)), (Some(0), Some(3))] {
        assert!(
            prepare_relational(&gap_fill(empty(fixture.scan()), &[], start, end), policy())
                .is_err()
        );
    }
    let prepared = prepare_relational(
        &gap_fill(fixture.scan(), &[], Some(0), Some(2_000_000_000)),
        policy(),
    )
    .unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let mut calls = 0;
    let failure = prepared.for_each_batch(&CancellationToken::default(), |array, _| {
        calls += 1;
        assert!(array.len() < 100_000);
        Err(shardloom_core::ShardLoomError::InvalidOperation(
            "consumer stop".into(),
        ))
    });
    assert!(failure.is_err());
    assert_eq!(calls, 1);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    for values in [[Some(0), Some(0)], [Some(0), Some(1)], [None, Some(2)]] {
        let bad = Fixture::new(
            StructArray::new(
                FieldNames::from(["instant"]),
                vec![temporal_array("timestamp(s)", &values)],
                2,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        );
        let prepared =
            prepare_relational(&gap_fill(bad.scan(), &[], None, None), policy()).unwrap();
        let baseline = prepared.session.memory().snapshot().reserved_bytes;
        assert!(
            prepared
                .execute_native(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
    }
}

#[test]
fn native_temporal_bucket_preserves_precision_nulls_schema_and_output_ownership() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["instant"]),
            vec![temporal_array(
                "timestamp(ns)",
                &[Some(-1), Some(0), Some(501), Some(1001), None],
            )],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let expressions = vec![
        (
            "anchored",
            function(
                "temporal_bucket",
                vec![col("instant"), number(500), text("ns"), number(1)],
            ),
        ),
        (
            "default",
            function(
                "temporal_bucket",
                vec![
                    col("instant"),
                    number(500),
                    text("ns"),
                    literal(ScalarValue::Null),
                ],
            ),
        ),
    ];
    let plan = project(fixture.scan(), expressions.clone());
    assert_eq!(
        collect(&plan),
        vec![
            json!({"anchored":-499,"default":-500}),
            json!({"anchored":-499,"default":0}),
            json!({"anchored":501,"default":500}),
            json!({"anchored":1001,"default":1000}),
            json!({"anchored":null,"default":null}),
        ]
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let expected = prepared.output_dtype().unwrap();
    for field in expected.as_struct_fields_opt().unwrap().fields() {
        assert_eq!(
            field,
            crate::native_temporal::vortex_dtype(&dtype("timestamp(ns)"), Nullability::Nullable)
                .unwrap()
        );
    }
    let empty = prepare_relational(&project(empty(fixture.scan()), expressions), policy()).unwrap();
    assert_eq!(empty.output_dtype().unwrap(), expected);
    let memory = prepared.session.memory().clone();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    drop((prepared, empty, fixture));
    assert_eq!(completed.result.dtype(), &expected);
    assert_eq!(completed.result.row_count(), 5);
    drop(completed);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_temporal_bucket_validates_every_literal_control_even_on_empty_inputs() {
    let fixture = decimals();
    let base = || vec![value("timestamp(ns)", 0), number(1), text("ns")];
    let mut bad = vec![];
    for argument in [
        number(-1),
        number(0),
        col("n"),
        text("1"),
        literal(ScalarValue::Float64(1.5)),
        literal(ScalarValue::Null),
    ] {
        let mut args = base();
        args[1] = argument;
        bad.push(args);
    }
    for unit in [text("guess"), col("text"), literal(ScalarValue::Null)] {
        let mut args = base();
        args[2] = unit;
        bad.push(args);
    }
    for origin in [
        col("n"),
        literal(ScalarValue::UInt64(u64::MAX)),
        text("1970-01-01"),
    ] {
        let mut args = base();
        args.push(origin);
        bad.push(args);
    }
    bad.extend([
        vec![value("timestamp(ms)", 0), number(1), text("ns")],
        vec![value("time(ns)", 0), number(1), text("calendar_days")],
        vec![value("date32", 0), number(1), text("s")],
        vec![
            value("date64", 0),
            number(1),
            text("calendar_days"),
            number(1),
        ],
        vec![
            value("timestamp(ns)", 0),
            number(1),
            text("ns"),
            number(0),
            text("clamp"),
        ],
        vec![
            value("timestamp(ns)", 0),
            number(1),
            text("ns"),
            number(0),
            text("reject"),
            text("earlier"),
        ],
    ]);
    for args in bad {
        assert!(
            prepare_relational(
                &project(
                    empty(fixture.scan()),
                    vec![("invalid", function("temporal_bucket", args))]
                ),
                policy()
            )
            .is_err()
        );
    }
}
