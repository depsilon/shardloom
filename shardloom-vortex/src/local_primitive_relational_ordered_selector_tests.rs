//! Complete typed aggregate selector references across native batches and ordered runs.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};

#[path = "local_primitive_relational_ordered_selector_window_tests.rs"]
mod windows;

fn order(name: &str, descending: bool) -> Vec<VortexRelationalOrderKey> {
    vec![VortexRelationalOrderKey {
        column: ColumnRef::new(name).unwrap(),
        descending,
        nulls: Some(VortexRelationalNullOrder::Last),
    }]
}

fn selectors(value: &str, key: &str) -> Vec<Measure> {
    ["first", "last", "arg_min", "arg_max"]
        .into_iter()
        .map(|function| {
            let measure = measure(function, Some(value), function);
            if function.starts_with("arg_") {
                measure.with_secondary_column(ColumnRef::new(key).unwrap())
            } else {
                measure
            }
        })
        .collect()
}

fn check(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[Value]) {
    let workspace = fixture.0.join("selector-runs");
    fs::create_dir_all(&workspace).unwrap();
    for spill in [false, true] {
        let prepared = prepare_relational(plan, policy()).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for batch_rows in [1, 3] {
            let mut rows = vec![];
            let report = prepared
                .for_each_json_batch(
                    &CancellationToken::default(),
                    batch_rows,
                    1 << 20,
                    |batch| {
                        rows.extend(
                            serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap(),
                        );
                        Ok(())
                    },
                )
                .unwrap_or_else(|error| panic!("spill={spill}, plan={plan:?}: {error}"));
            assert_eq!(rows, expected, "spill={spill}, batch={batch_rows}");
            assert!(report.native_io_certificate.is_certified());
            assert!(!report.native_io_certificate.side_effects.fallback_attempted);
            if spill {
                assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
            }
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn ordered_selector_groups_preserve_null_values_exact_keys_and_declared_tie_order() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "value", "key", "order"]),
            vec![
                VarBinViewArray::from_iter_str(["B", "A", "B", "A", "B", "C", "C", "D", "D"])
                    .into_array(),
                VarBinViewArray::from_iter_nullable_str([
                    None,
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                    None,
                    None,
                    Some("u"),
                    Some("v"),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(0u64),
                    Some(9),
                    Some(2),
                    Some(9),
                    Some(2),
                    None,
                    Some(8),
                    Some(u64::MAX),
                    Some(u64::MAX - 1),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(3i32),
                    Some(2),
                    Some(5),
                    Some(1),
                    Some(1),
                    Some(9),
                    Some(0),
                    None,
                    Some(4),
                ])
                .into_array(),
            ],
            9,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let mut measures = selectors("value", "key");
    for mut measure in selectors("value", "key") {
        measure.alias.push_str("_ordered");
        measures.push(measure.with_order_by(order("order", false)));
    }
    let plan = aggregate(fixture.scan(), &["group"], measures);
    check(
        &fixture,
        &plan,
        &[
            json!({"group":"B","first":null,"last":"d","arg_min":"b","arg_max":"b","first_ordered":"d","last_ordered":"b","arg_min_ordered":"d","arg_max_ordered":"d"}),
            json!({"group":"A","first":"a","last":"c","arg_min":"a","arg_max":"a","first_ordered":"c","last_ordered":"a","arg_min_ordered":"c","arg_max_ordered":"c"}),
            json!({"group":"C","first":null,"last":null,"arg_min":null,"arg_max":null,"first_ordered":null,"last_ordered":null,"arg_min_ordered":null,"arg_max_ordered":null}),
            json!({"group":"D","first":"u","last":"v","arg_min":"v","arg_max":"u","first_ordered":"v","last_ordered":"u","arg_min_ordered":"v","arg_max_ordered":"u"}),
        ],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let dtype = prepared.output_dtype().unwrap();
    let fields = dtype.as_struct_fields_opt().unwrap();
    for name in ["first", "last", "arg_min", "arg_max"] {
        assert_eq!(
            fields.field(name).unwrap(),
            DType::Utf8(Nullability::Nullable)
        );
    }
}

#[test]
fn ordered_selector_empty_null_and_original_uint64_outputs_keep_their_types() {
    for values in [
        vec![],
        vec![None, None],
        vec![Some(u64::MAX), None, Some(u64::MAX - 1)],
    ] {
        let observed = values.iter().flatten().copied().collect::<Vec<_>>();
        let expected = json!({"first":values.first().copied().flatten(),"last":values.last().copied().flatten(),
            "arg_min":observed.iter().min(),"arg_max":observed.iter().max()});
        let fixture = Fixture::new(
            single(
                "value",
                PrimitiveArray::from_option_iter(values).into_array(),
            ),
            1,
        );
        let plan = aggregate(fixture.scan(), &[], selectors("value", "value"));
        check(&fixture, &plan, &[expected]);
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let dtype = prepared.output_dtype().unwrap();
        for name in ["first", "last", "arg_min", "arg_max"] {
            assert_eq!(
                dtype.as_struct_fields_opt().unwrap().field(name).unwrap(),
                DType::Primitive(PType::U64, Nullability::Nullable)
            );
        }
    }
}

#[test]
fn ordered_selector_native_nested_values_keep_null_parents_and_children() {
    use vortex::array::{VortexSessionExecute as _, arrays::ListArray};
    let values = ListArray::try_new(
        PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(7)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 3]).into_array(),
        Validity::from_iter([true, false, true]),
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let dtype = values.dtype().clone();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["value", "key"]),
            vec![values, PrimitiveArray::from_iter([2i32, 0, 1]).into_array()],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let plan = aggregate(fixture.scan(), &[], selectors("value", "key"));
    check(
        &fixture,
        &plan,
        &[json!({"first":[u64::MAX,null],"last":[7],"arg_min":[7],"arg_max":[u64::MAX,null]})],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let output = prepared.output_dtype().unwrap();
    for name in ["first", "last", "arg_min", "arg_max"] {
        assert_eq!(
            output.as_struct_fields_opt().unwrap().field(name).unwrap(),
            dtype
        );
    }
}

#[test]
fn ordered_selector_argument_and_comparison_failures_are_explicit_and_refund_state() {
    let fixture = fixture();
    let invalid = [
        measure("arg_min", Some("number"), "bad"),
        measure("first", Some("number"), "bad")
            .with_secondary_column(ColumnRef::new("word").unwrap()),
        measure("sum", Some("number"), "bad").with_order_by(order("word", false)),
        measure("first", Some("number"), "bad").with_order_by(order("missing", false)),
    ];
    for measure in invalid {
        assert!(
            prepare_relational(&aggregate(fixture.scan(), &[], vec![measure]), policy()).is_err()
        );
    }
    let mut ordering = order("number", false);
    ordering[0].nulls = None;
    let plan = aggregate(
        fixture.scan(),
        &[],
        vec![measure("first", Some("word"), "value").with_order_by(ordering)],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("NULLS FIRST or NULLS LAST"), "{error}");
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn ordered_selector_forced_spill_keeps_input_ordinals_and_exact_extreme_ties() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["value", "key"]),
            vec![
                PrimitiveArray::from_iter(0u64..65_536).into_array(),
                PrimitiveArray::from_iter((0u64..65_536).map(|row| (row * 73) % 1024)).into_array(),
            ],
            65_536,
            Validity::NonNullable,
        )
        .into_array(),
        1024,
    );
    let mut measures = selectors("value", "key");
    for mut measure in selectors("value", "key") {
        measure.alias.push_str("_descending");
        measures.push(measure.with_order_by(order("value", true)));
    }
    let workspace = fixture.0.join("forced-selectors");
    fs::create_dir(&workspace).unwrap();
    let prepared = prepare_relational(&aggregate(fixture.scan(), &[], measures), policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let first_max = (0u64..65_536)
        .find(|row| (row * 73) % 1024 == 1023)
        .unwrap();
    let last_max = (0u64..65_536)
        .rev()
        .find(|row| (row * 73) % 1024 == 1023)
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            json!({"first":0,"last":65_535,"arg_min":0,"arg_max":first_max,
        "first_descending":65_535,"last_descending":0,"arg_min_descending":64_512,"arg_max_descending":last_max})
        ]
    );
    assert!(result.execution.spill.as_ref().unwrap().runs_written > 0);
    assert!(
        result
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
}
