use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalNullOrder,
        VortexRelationalOrderKey, VortexRelationalSort,
    },
};
use vortex::array::arrays::VarBinViewArray;

fn measure(function: &str, column: Option<&str>, alias: &str) -> Measure {
    Measure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}
fn aggregate(
    input: VortexRelationalPlan,
    groups: &[&str],
    measures: Vec<Measure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
        measures,
    }))
}

fn fixture() -> Fixture {
    Fixture::new(
        StructArray::try_new(
            FieldNames::from(["group", "number", "word"]),
            vec![
                VarBinViewArray::from_iter_nullable_str([
                    Some("B"),
                    Some("A"),
                    Some("B"),
                    None,
                    Some("A"),
                    Some("B"),
                    None,
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(-2_i16),
                    Some(5),
                    None,
                    Some(9),
                    Some(5),
                    Some(4),
                    None,
                ])
                .into_array(),
                VarBinViewArray::from_iter_nullable_str([
                    Some("Z"),
                    Some("é"),
                    Some("Z"),
                    None,
                    Some("é"),
                    Some("ß"),
                    Some(""),
                ])
                .into_array(),
            ],
            7,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    )
}

fn all_measures() -> Vec<Measure> {
    vec![
        measure("count", None, "rows"),
        measure("count", Some("number"), "present"),
        measure("count_distinct", Some("number"), "distinct"),
        measure("sum", Some("number"), "sum"),
        measure("avg", Some("number"), "avg"),
        measure("min", Some("number"), "min"),
        measure("max", Some("number"), "max"),
        measure("count_distinct", Some("word"), "words"),
        measure("min", Some("word"), "first"),
        measure("max", Some("word"), "last"),
    ]
}

#[test]
fn native_relational_aggregate_exact_grouping_and_distinct_across_batches_preserve_nulls_and_order()
{
    let fixture = fixture();
    let plan = aggregate(fixture.scan(), &["group"], all_measures());
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let expected = serde_json::json!([
        {"group":"B","rows":3,"present":2,"distinct":2,"sum":2.0,"avg":1.0,"min":-2,"max":4,"words":2,"first":"Z","last":"ß"},
        {"group":"A","rows":2,"present":2,"distinct":1,"sum":10.0,"avg":5.0,"min":5,"max":5,"words":1,"first":"é","last":"é"},
        {"group":null,"rows":2,"present":1,"distinct":1,"sum":9.0,"avg":9.0,"min":9,"max":9,"words":1,"first":"","last":""}
    ]);
    for call in 1..=3 {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(serde_json::json!(json_rows(&collected)), expected);
        assert_eq!(collected.execution.runtime.prepared_source_opens, 1);
        assert_eq!(collected.execution.runtime.completed_executions, call);
        drop(collected);
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
    }
    assert_eq!(
        prepared
            .output_dtype()
            .unwrap()
            .as_struct_fields_opt()
            .unwrap()
            .field("min")
            .unwrap(),
        DType::Primitive(PType::I16, Nullability::Nullable)
    );
}

#[test]
fn native_relational_aggregate_empty_scalar_and_grouped_results_have_authoritative_schema() {
    let fixture = fixture();
    let empty = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["group", "number", "word"]),
            vec![
                VarBinViewArray::from_iter_nullable_str([] as [Option<&str>; 0]).into_array(),
                PrimitiveArray::from_option_iter([] as [Option<i16>; 0]).into_array(),
                VarBinViewArray::from_iter_nullable_str([] as [Option<&str>; 0]).into_array(),
            ],
            0,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let scalar =
        prepare_relational(&aggregate(empty.scan(), &[], all_measures()), policy()).unwrap();
    assert_eq!(
        json_rows(&scalar.collect_jsonl(&CancellationToken::default()).unwrap()),
        vec![
            serde_json::json!({"rows":0,"present":0,"distinct":0,"sum":null,"avg":null,"min":null,"max":null,"words":0,"first":null,"last":null})
        ]
    );
    let grouped = prepare_relational(
        &aggregate(empty.scan(), &["group"], all_measures()),
        policy(),
    )
    .unwrap();
    let full = prepare_relational(
        &aggregate(fixture.scan(), &["group"], all_measures()),
        policy(),
    )
    .unwrap();
    assert_eq!(
        grouped.output_dtype().unwrap(),
        full.output_dtype().unwrap()
    );
    assert_eq!(grouped.execute_owned().unwrap().execution.output_rows, 0);
    let invalid = aggregate(empty.scan(), &[], vec![measure("sum", Some("word"), "bad")]);
    assert!(prepare_relational(&invalid, policy()).is_err());
}

#[test]
fn native_relational_aggregate_unsigned_extrema_float_zero_distinct_and_ordered_sum_are_exact() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["wide", "zero", "float"]),
            vec![
                PrimitiveArray::from_iter([u64::MAX, 0, i64::MAX.cast_unsigned()]).into_array(),
                PrimitiveArray::from_iter([-0.0_f64, 0.0, -0.0]).into_array(),
                PrimitiveArray::from_iter([1e16_f64, -1e16, 1.0]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let prepared = prepare_relational(
        &aggregate(
            fixture.scan(),
            &[],
            vec![
                measure("max", Some("wide"), "max"),
                measure("min", Some("wide"), "min"),
                measure("count_distinct", Some("zero"), "zeros"),
                measure("min", Some("zero"), "first_zero"),
                measure("sum", Some("float"), "ordered_sum"),
            ],
        ),
        policy(),
    )
    .unwrap();
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![
            serde_json::json!({"max":u64::MAX,"min":0,"zeros":1,"first_zero":-0.0,"ordered_sum":1.0})
        ]
    );
    prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            let zero =
                crate::local_primitives::logical_field_from_native_array(&array, "first_zero")?;
            let value = result_batch::scalar_value(
                &zero,
                0,
                &mut context.native_session().create_execution_ctx(),
            )?;
            let result_batch::Value::Float(value) = value else {
                panic!("expected float")
            };
            assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
            Ok(())
        })
        .unwrap();
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_join_aggregate_having_and_sort_compose_through_all_writers() {
    use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    let grouped = aggregate(
        join(&left, &right, JoinKind::Full),
        &["debit"],
        vec![
            measure("sum", Some("credit"), "sum"),
            measure("count", Some("credit"), "present"),
            measure("count_distinct", Some("credit"), "distinct"),
        ],
    );
    let predicate = Expression::new(
        ExprId::new("having").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new("sum").unwrap(),
                ColumnRef::new("sum").unwrap(),
            )),
            op: ComparisonOp::Gt,
            right: Box::new(Expression::literal(
                ExprId::new("minimum").unwrap(),
                ScalarValue::Float64(20.0),
            )),
        },
    );
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: grouped,
        predicate,
    }));
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: filtered,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("sum").unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    super::writer_tests::verify_writers(
        &left,
        &sorted,
        "join-aggregate",
        &[
            serde_json::json!({"debit":10,"sum":41.0,"present":2,"distinct":2}),
            serde_json::json!({"debit":null,"sum":22.0,"present":1,"distinct":1}),
        ],
        "debit,sum,present,distinct\n10,41,2,2\n,22,1,1\n",
    );
}

#[test]
fn native_relational_aggregate_pressure_denial_and_cancel_release_all_query_state() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter(0..5000_u64).into_array()),
        250,
    );
    let prepared = prepare_relational(
        &aggregate(
            fixture.scan(),
            &["value"],
            vec![
                measure("count", None, "count"),
                measure("count_distinct", Some("value"), "distinct"),
            ],
        ),
        policy(),
    )
    .unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let block = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - 128 * 1024)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(block);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    let token = CancellationToken::default();
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                delivered += 1;
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.execution.output_rows, 5000);
    assert_eq!(result.execution.runtime.prepared_source_opens, 1);
    assert_eq!(result.execution.runtime.completed_executions, 1);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_grouped_aggregate_writes_complete_results_above_collection_bounds() {
    use std::fmt::Write as _;
    let fixture = Fixture::new(
        single("key", PrimitiveArray::from_iter(0..65_537_u32).into_array()),
        4096,
    );
    let plan = aggregate(
        fixture.scan(),
        &["key"],
        vec![measure("count", None, "rows")],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert!(prepared.execute_owned().is_err());
    let expected = (0..65_537_u32)
        .map(|key| serde_json::json!({"key":key,"rows":1}))
        .collect::<Vec<_>>();
    let mut csv = String::from("key,rows\n");
    for key in 0..65_537_u32 {
        writeln!(&mut csv, "{key},1").unwrap();
    }
    super::writer_tests::verify_writers(&fixture, &plan, "large-aggregate", &expected, &csv);
}
