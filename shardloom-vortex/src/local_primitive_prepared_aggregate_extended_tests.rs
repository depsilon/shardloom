use super::*;
use crate::VortexAggregateExpression;
use vortex::array::{ArrayRef, arrays::VarBinViewArray};

fn write_columns(fixture: &Fixture, names: &[&str], columns: Vec<ArrayRef>, rows: usize) {
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::try_new(
        FieldNames::from(names.to_vec()),
        columns,
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut output = std::fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .blocking(&runtime)
        .writer(&mut output, array.dtype().clone());
    // Multiple chunks make retained preparation cross the ordinary scan boundary.
    for start in (0..rows).step_by(2) {
        writer
            .push(array.slice(start..(start + 2).min(rows)).unwrap())
            .unwrap();
    }
    writer.finish().unwrap();
}

fn assert_repeated(request: &VortexQueryPrimitiveRequest, expected: &serde_json::Value) {
    for parallelism in [1, 4] {
        let policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        let ordinary =
            crate::local_primitives::execute_vortex_local_primitive_with_policy(request, policy)
                .unwrap();
        assert_eq!(payload(&ordinary)["values"], *expected);
        let prepared = prepare_aggregate(request, policy).unwrap();
        let owner = prepared.session.clone();
        assert_eq!(owner.snapshot().completed_executions, 0);
        for execution in 1..=3 {
            let result = prepared.execute().unwrap();
            certified(&result, execution);
            assert_eq!(payload(&result.report)["values"], *expected);
        }
        for execution in 4..=6 {
            let collected = prepared
                .collect_jsonl(&shardloom_exec::compute_pool::CancellationToken::default())
                .unwrap();
            certified(&collected.execution, execution);
            let actual: Vec<serde_json::Value> = collected
                .result_jsonl
                .value()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(serde_json::Value::Array(actual), *expected);
            assert_eq!(
                collected
                    .execution
                    .native_io_certificate
                    .sink_requirement_report
                    .target_format,
                "bounded_json_rows"
            );
            assert!(
                owner.snapshot().memory.reserved_bytes
                    >= collected.result_jsonl.value().capacity() as u64
            );
        }
        drop(prepared);
        assert_eq!(owner.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn prepared_aggregate_explicit_null_order_keeps_measures_keys_and_windows_exact() {
    use crate::relational_query::VortexRelationalNullOrder::{First, Last};
    let fixture = Fixture::new();
    write_columns(
        &fixture,
        &["cohort", "reading"],
        vec![
            VarBinViewArray::from_iter_nullable_str([
                None,
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("東京"),
                None,
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("東京"),
            ])
            .into_array(),
            PrimitiveArray::from_option_iter([
                Some(1_i64),
                None,
                Some(2),
                None,
                Some(1),
                Some(-5),
                Some(7),
                None,
                Some(6),
                None,
                Some(7),
                Some(-1),
            ])
            .into_array(),
        ],
        12,
    );
    let rows = serde_json::json!([
        {"cohort":null,"mean":4.0,"entries":2},
        {"cohort":"a","mean":null,"entries":2},
        {"cohort":"b","mean":4.0,"entries":2},
        {"cohort":"c","mean":null,"entries":2},
        {"cohort":"d","mean":4.0,"entries":2},
        {"cohort":"東京","mean":-3.0,"entries":2},
    ]);
    // Full independently specified output order, including ties and all-null groups.
    for (descending, nulls, indices) in [
        (false, First, [1, 3, 5, 2, 4, 0]),
        (true, First, [1, 3, 2, 4, 0, 5]),
        (false, Last, [5, 2, 4, 0, 1, 3]),
        (true, Last, [2, 4, 0, 5, 1, 3]),
    ] {
        for limited in [false, true] {
            let mut aggregate = VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("cohort").unwrap()],
                vec![
                    measure("avg", Some("reading"), "mean"),
                    measure("count", None, "entries"),
                ],
            )
            .with_order_by(vec![
                VortexAggregateOrderExpr::new("mean", descending).with_nulls(nulls),
                VortexAggregateOrderExpr::new("cohort", false).with_nulls(Last),
            ]);
            if limited {
                aggregate.offset = 1;
            }
            let mut request = fixture.request(aggregate);
            if limited {
                request.source_order_limit = Some(3);
            }
            let indices = if limited {
                &indices[1..4]
            } else {
                &indices[..]
            };
            let expected = serde_json::Value::Array(
                indices.iter().map(|&index| rows[index].clone()).collect(),
            );
            assert_repeated(&request, &expected);
        }
    }
}

#[test]
fn prepared_complete_triple_keys_preserve_nulls_and_mixed_measure_values() {
    let fixture = Fixture::new();
    write_columns(
        &fixture,
        &["account", "bucket", "term", "amount"],
        vec![
            PrimitiveArray::new(vec![1_i64, 1, 1, 2, 2], Validity::NonNullable).into_array(),
            PrimitiveArray::new(vec![7_i16, 7, 8, 7, 7], Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_nullable_str([Some("a"), Some("a"), Some("a"), None, None])
                .into_array(),
            PrimitiveArray::from_option_iter([
                Some(1.5_f64),
                None,
                Some(2.5),
                Some(-1.0),
                Some(3.0),
            ])
            .into_array(),
        ],
        5,
    );
    let keys = ["account", "bucket", "term"];
    let request = fixture.request(
        VortexSimpleAggregateRequest::grouped(
            keys.map(|key| ColumnRef::new(key).unwrap()).to_vec(),
            vec![
                measure("count", None, "rows"),
                measure("count", Some("amount"), "present"),
                measure("sum", Some("amount"), "total"),
                measure("avg", Some("amount"), "mean"),
            ],
        )
        .with_order_by(
            keys.map(|key| VortexAggregateOrderExpr::new(key, false))
                .to_vec(),
        ),
    );
    assert_repeated(
        &request,
        &serde_json::json!([
            {"account":1,"bucket":7,"term":"a","rows":2,"present":1,"total":1.5,"mean":1.5},
            {"account":1,"bucket":8,"term":"a","rows":1,"present":1,"total":2.5,"mean":2.5},
            {"account":2,"bucket":7,"term":null,"rows":2,"present":2,"total":2.0,"mean":1.0}
        ]),
    );
}

#[test]
fn prepared_derived_keys_and_transformed_measures_use_existing_lowering() {
    let fixture = Fixture::new();
    write_columns(
        &fixture,
        &["bucket", "label", "amount"],
        vec![
            PrimitiveArray::new(vec![2_i64, 2, 3, 3], Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_nullable_str([Some("é"), None, Some("abc"), Some("")])
                .into_array(),
            PrimitiveArray::from_option_iter([Some(1_i64), Some(3), None, Some(5)]).into_array(),
        ],
        4,
    );
    let request = fixture.request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("bucket").unwrap()],
            vec![
                measure("count", None, "rows"),
                measure("sum", Some("amount"), "adjusted").with_argument_offset(2),
                measure("sum", Some("label"), "bytes").with_value_transform("length"),
            ],
        )
        .with_group_expressions(vec![
            VortexAggregateExpression::new(
                "prior".into(),
                ColumnRef::new("bucket").unwrap(),
                "add_offset",
            )
            .with_argument_offset(-1),
        ])
        .with_order_by(vec![VortexAggregateOrderExpr::new("bucket", false)]),
    );
    assert_repeated(
        &request,
        &serde_json::json!([
            {"bucket":2,"prior":1,"rows":2,"adjusted":8.0,"bytes":2.0},
            {"bucket":3,"prior":2,"rows":2,"adjusted":7.0,"bytes":3.0}
        ]),
    );
}

#[test]
fn prepared_wide_measure_set_preserves_offsets_and_ordered_accumulation() {
    let fixture = Fixture::new();
    let measures = (0..90)
        .map(|offset| {
            measure("sum", Some("metric"), &format!("measure_{offset}"))
                .with_argument_offset(offset)
        })
        .collect();
    let expected: serde_json::Map<_, _> = (0..90)
        .map(|offset| {
            (
                format!("measure_{offset}"),
                serde_json::json!(150.0 + 5.0 * f64::from(offset)),
            )
        })
        .collect();
    assert_repeated(
        &fixture.request(VortexSimpleAggregateRequest::new(measures)),
        &serde_json::Value::Object(expected),
    );
}

#[test]
fn prepared_residual_state_is_evaluated_again_on_each_execution() {
    let fixture = Fixture::new();
    let request = fixture.request(scalar());
    let mut prepared = prepare_aggregate(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    // Current public predicates all push down. Exercise the shared lowered
    // residual contract directly, without inventing an unsupported predicate.
    prepared.lowering.residual = Some(PredicateExpr::Compare {
        column: ColumnRef::new("metric").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(20),
    });
    for execution in 1..=3 {
        let result = prepared.execute().unwrap();
        certified(&result, execution);
        assert_eq!(
            payload(&result.report)["values"],
            serde_json::json!({"rows_alias":3,"unique_alias":3,"total_alias":120.0})
        );
    }
}
