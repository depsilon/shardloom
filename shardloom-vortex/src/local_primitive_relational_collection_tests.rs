//! Complete list/string references share the native resident and stored paths.

use super::*;

#[path = "local_primitive_relational_collection_lifecycle_tests.rs"]
mod lifecycle;

fn collection_measures(value: &str, text: &str) -> Vec<Measure> {
    vec![
        measure("list", Some(value), "original"),
        measure("array_agg", Some(value), "ascending").with_order_by(order("local_order", false)),
        measure("list", Some(value), "descending").with_order_by(order("local_order", true)),
        measure("string_agg", Some(text), "joined").with_separator(" / "),
        measure("string_agg", Some(text), "quoted")
            .with_separator("'東京,")
            .with_order_by(order("local_order", false)),
        measure("string_agg", Some(text), "tight")
            .with_separator(String::new())
            .with_order_by(order("local_order", true)),
    ]
}

fn joined(values: &[Option<&str>], rows: &[usize], separator: &str) -> Value {
    let observed = rows
        .iter()
        .filter_map(|&row| values[row])
        .collect::<Vec<_>>();
    if observed.is_empty() {
        Value::Null
    } else {
        json!(observed.join(separator))
    }
}

fn collected<T: serde::Serialize>(values: &[T], rows: &[usize]) -> Value {
    if rows.is_empty() {
        Value::Null
    } else {
        json!(rows.iter().map(|&row| &values[row]).collect::<Vec<_>>())
    }
}

#[test]
fn ordered_collection_groups_preserve_duplicate_null_large_integer_and_utf8_values() {
    let groups = ["B", "A", "B", "A", "B", "C", "C", "D", "D"];
    let values = [
        Some(u64::MAX),
        None,
        Some(7),
        Some(8),
        Some(7),
        None,
        None,
        Some(3),
        Some(4),
    ];
    let text = [
        None,
        Some("a"),
        Some(""),
        Some("東京"),
        Some("'"),
        None,
        None,
        Some(""),
        Some("Ω"),
    ];
    let ordering = [
        Some(3i32),
        Some(2),
        Some(5),
        Some(1),
        Some(1),
        Some(9),
        Some(0),
        None,
        Some(4),
    ];
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "value", "text", "local_order"]),
            vec![
                VarBinViewArray::from_iter_str(groups).into_array(),
                PrimitiveArray::from_option_iter(values).into_array(),
                VarBinViewArray::from_iter_nullable_str(text).into_array(),
                PrimitiveArray::from_option_iter(ordering).into_array(),
            ],
            groups.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let expected = ["B", "A", "C", "D"].into_iter().map(|group| {
        let original = (0..groups.len()).filter(|&row| groups[row] == group).collect::<Vec<_>>();
        let ordered = |descending| {
            let mut rows = original.clone();
            rows.sort_by_key(|&row| (ordering[row].is_none(),
                if descending { -ordering[row].unwrap_or_default() } else { ordering[row].unwrap_or_default() }, row));
            rows
        };
        let ascending = ordered(false);
        let descending = ordered(true);
        json!({"group":group,"original":collected(&values,&original),
            "ascending":collected(&values,&ascending),"descending":collected(&values,&descending),
            "joined":joined(&text,&original," / "),"quoted":joined(&text,&ascending,"'東京,"),
            "tight":joined(&text,&descending,""),"count":original.iter().filter(|&&row| values[row].is_some()).count()})
    }).collect::<Vec<_>>();
    let mut measures = collection_measures("value", "text");
    measures.push(measure("count", Some("value"), "count"));
    let plan = aggregate(fixture.scan(), &["group"], measures);
    check(&fixture, &plan, &expected);
    let dtype = prepare_relational(&plan, policy())
        .unwrap()
        .output_dtype()
        .unwrap();
    let fields = dtype.as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.field("original").unwrap(),
        DType::List(
            DType::Primitive(PType::U64, Nullability::Nullable).into(),
            Nullability::Nullable
        )
    );
    assert_eq!(
        fields.field("joined").unwrap(),
        DType::Utf8(Nullability::Nullable)
    );
}

#[test]
fn ordered_collection_windows_match_all_frame_units_exclusions_and_independent_local_orders() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "value", "local_order"]),
            vec![
                PrimitiveArray::from_iter(IDS).into_array(),
                PrimitiveArray::from_iter(WINDOW_KEYS).into_array(),
                VarBinViewArray::from_iter_nullable_str(VALUES).into_array(),
                PrimitiveArray::from_option_iter(LOCAL_ORDER).into_array(),
            ],
            IDS.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
        for exclusion in [
            Exclusion::NoOthers,
            Exclusion::CurrentRow,
            Exclusion::Group,
            Exclusion::Ties,
        ] {
            let distance = if unit == Unit::Rows { 2 } else { 1 };
            let frame = Frame {
                unit,
                exclusion,
                start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(distance))),
                end: Bound::Following(Offset::Number(ScalarValue::UInt64(distance))),
            };
            let expected = IDS.iter().enumerate().map(|(row, &id)| {
                let mut original = frame_members(unit, exclusion, row);
                original.sort_by_key(|row| PARTITION_ORDER.iter().position(|candidate| candidate == row).unwrap());
                let ascending = local_ordered(original.clone(), false);
                let descending = local_ordered(original.clone(), true);
                json!({"id":id,"original":collected(&VALUES,&original),
                    "ascending":collected(&VALUES,&ascending),"descending":collected(&VALUES,&descending),
                    "joined":joined(&VALUES,&original," / "),"quoted":joined(&VALUES,&ascending,"'東京,"),
                    "tight":joined(&VALUES,&descending,"")})
            }).collect::<Vec<_>>();
            let plan = window_plan(&fixture, &frame, collection_measures("value", "value"));
            check(&fixture, &plan, &expected);
        }
    }
}

#[test]
fn ordered_collection_empty_domains_and_nonnullable_children_remain_distinct() {
    for values in [vec![], vec![u64::MAX], vec![1, 2, 2]] {
        let fixture = Fixture::new(
            single(
                "value",
                PrimitiveArray::from_iter(values.clone()).into_array(),
            ),
            1,
        );
        let plan = aggregate(
            fixture.scan(),
            &[],
            vec![measure("list", Some("value"), "items")],
        );
        check(
            &fixture,
            &plan,
            &[json!({"items":if values.is_empty() { Value::Null } else { json!(values) }})],
        );
        let dtype = prepare_relational(&plan, policy())
            .unwrap()
            .output_dtype()
            .unwrap();
        assert_eq!(
            dtype
                .as_struct_fields_opt()
                .unwrap()
                .field("items")
                .unwrap(),
            DType::List(
                DType::Primitive(PType::U64, Nullability::NonNullable).into(),
                Nullability::Nullable
            )
        );
    }
    for values in [
        vec![],
        vec![None, None],
        vec![Some("")],
        vec![Some(""), Some("")],
    ] {
        let fixture = Fixture::new(
            single(
                "value",
                VarBinViewArray::from_iter_nullable_str(values.clone()).into_array(),
            ),
            1,
        );
        let plan = aggregate(
            fixture.scan(),
            &[],
            vec![
                measure("list", Some("value"), "items"),
                measure("string_agg", Some("value"), "joined").with_separator("'東京,"),
            ],
        );
        let rows = (0..values.len()).collect::<Vec<_>>();
        check(
            &fixture,
            &plan,
            &[json!({"items":collected(&values,&rows),"joined":joined(&values,&rows,"'東京,")})],
        );
    }
}

#[test]
fn ordered_collection_nested_lists_preserve_parent_and_child_nulls() {
    use vortex::array::{VortexSessionExecute as _, arrays::ListArray};
    let source = ListArray::try_new(
        PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(7)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 3]).into_array(),
        Validity::from_iter([true, false, true]),
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let dtype = source.dtype().clone();
    let fixture = Fixture::new(single("value", source), 1);
    let plan = aggregate(
        fixture.scan(),
        &[],
        vec![measure("array_agg", Some("value"), "items")],
    );
    check(
        &fixture,
        &plan,
        &[json!({"items":[[u64::MAX,null],null,[7]]})],
    );
    let output = prepare_relational(&plan, policy())
        .unwrap()
        .output_dtype()
        .unwrap();
    assert_eq!(
        output
            .as_struct_fields_opt()
            .unwrap()
            .field("items")
            .unwrap(),
        DType::List(dtype.into(), Nullability::Nullable)
    );
}

#[test]
fn ordered_collection_string_nulls_skip_local_keys_while_lists_observe_them() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "value", "local_order"]),
            vec![
                PrimitiveArray::from_iter(0u64..3).into_array(),
                PrimitiveArray::from_iter(0u64..3).into_array(),
                VarBinViewArray::from_iter_nullable_str([None, Some(""), Some("ok")]).into_array(),
                PrimitiveArray::from_iter([f64::NAN, 2.0, 1.0]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let measure = measure("string_agg", Some("value"), "joined")
        .with_separator("/")
        .with_order_by(order("local_order", false));
    check(
        &fixture,
        &aggregate(fixture.scan(), &[], vec![measure.clone()]),
        &[json!({"joined":"ok/"})],
    );
    let frame = Frame {
        unit: Unit::Rows,
        start: Bound::UnboundedPreceding,
        end: Bound::UnboundedFollowing,
        exclusion: Exclusion::NoOthers,
    };
    check(
        &fixture,
        &window_plan(&fixture, &frame, vec![measure]),
        &(0..3)
            .map(|id| json!({"id":id,"joined":"ok/"}))
            .collect::<Vec<_>>(),
    );
    let list = super::super::measure("list", Some("value"), "items")
        .with_order_by(order("local_order", false));
    for plan in [
        aggregate(fixture.scan(), &[], vec![list.clone()]),
        window_plan(&fixture, &frame, vec![list]),
    ] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let error = prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("finite"), "{error}");
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
