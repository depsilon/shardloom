use super::*;
use vortex::array::{
    arrays::{DecimalArray, ExtensionArray, FixedSizeListArray, ListViewArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

fn lists(rows: &[Option<Vec<Option<i64>>>]) -> ArrayRef {
    let mut children = Vec::new();
    let mut offsets = Vec::new();
    let mut sizes = Vec::new();
    for row in rows {
        offsets.push(children.len() as u64);
        let row = row.as_deref().unwrap_or_default();
        sizes.push(row.len() as u64);
        children.extend_from_slice(row);
    }
    ListViewArray::try_new(
        PrimitiveArray::from_option_iter(children).into_array(),
        PrimitiveArray::from_iter(offsets).into_array(),
        PrimitiveArray::from_iter(sizes).into_array(),
        Validity::from_iter(rows.iter().map(Option::is_some)),
    )
    .unwrap()
    .into_array()
}

fn typed_values() -> Vec<(ArrayRef, Vec<Value>)> {
    let maximum = 10i128.pow(38) - 1;
    let values = (0..2).flat_map(|_| 0..257).collect::<Vec<_>>();
    let binary = values
        .iter()
        .map(|index| {
            (index % 3 != 0).then_some(if index % 2 == 0 {
                &b"\0\xff\xc3\xa9"[..]
            } else {
                &b""[..]
            })
        })
        .collect::<Vec<_>>();
    let decimal = values
        .iter()
        .map(|index| (index % 3 != 0).then_some(if index % 2 == 0 { maximum } else { -maximum }));
    let dates = values
        .iter()
        .map(|index| (index % 3 != 0).then_some(if index % 2 == 0 { i32::MAX } else { i32::MIN }));
    let timestamps = values
        .iter()
        .map(|index| (index % 3 != 0).then_some(if index % 2 == 0 { i64::MAX } else { i64::MIN }));
    let list_values = values
        .iter()
        .map(|index| (index % 3 != 0).then(|| vec![Some(i64::from(*index)), None, Some(i64::MAX)]))
        .collect::<Vec<_>>();
    vec![
        (
            VarBinArray::from(binary).into_array(),
            (0..257)
                .map(|index| {
                    if index % 3 == 0 {
                        Value::Null
                    } else if index % 2 == 0 {
                        json!("00ffc3a9")
                    } else {
                        json!("")
                    }
                })
                .collect(),
        ),
        (
            DecimalArray::from_option_iter(decimal, DecimalDType::new(38, 6)).into_array(),
            (0..257)
                .map(|index| {
                    if index % 3 == 0 {
                        Value::Null
                    } else {
                        json!(format!(
                            "decimal128(38,6):{}",
                            if index % 2 == 0 { maximum } else { -maximum }
                        ))
                    }
                })
                .collect(),
        ),
        (
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter(dates).into_array(),
            )
            .into_array(),
            (0..257)
                .map(|index| {
                    if index % 3 == 0 {
                        Value::Null
                    } else {
                        json!(if index % 2 == 0 { i32::MAX } else { i32::MIN })
                    }
                })
                .collect(),
        ),
        (
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter(timestamps).into_array(),
            )
            .into_array(),
            (0..257)
                .map(|index| {
                    if index % 3 == 0 {
                        Value::Null
                    } else {
                        json!(if index % 2 == 0 { i64::MAX } else { i64::MIN })
                    }
                })
                .collect(),
        ),
        (
            lists(&list_values),
            list_values[..257]
                .iter()
                .map(|value| json!(value))
                .collect(),
        ),
    ]
}

#[test]
fn native_pivot_spill_exact_binary_decimal_temporal_and_list_values_survive_duplicate_reads() {
    for (values, expected) in typed_values() {
        let dtype = values.dtype().clone();
        let fixture = table(
            VarBinArray::from((0..2).flat_map(|_| (0..257).map(key)).collect::<Vec<_>>())
                .into_array(),
            VarBinArray::from(vec!["a"; 514]).into_array(),
            values,
            67,
        );
        for aggregate in ["first", "first_unique"] {
            let plan = plan(&fixture, aggregate);
            let resident = prepare(&fixture, plan.clone(), false);
            let prepared = prepare(&fixture, plan, true);
            let expected = expected
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    json!({
                        "entity":key(index), "pivot_a":value,
                    })
                })
                .collect::<Vec<_>>();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let (actual, report) = complete(&prepared);
            assert_eq!(actual, expected, "{dtype}/{aggregate}");
            assert_eq!(complete(&resident).0, expected);
            assert!(report.spill.as_ref().unwrap().merge_passes > 0);
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            let owned = prepared.execute_owned().unwrap();
            assert_eq!(
                owned
                    .result
                    .dtype()
                    .as_struct_fields_opt()
                    .unwrap()
                    .field("pivot_a"),
                Some(dtype.as_nullable())
            );
            let memory = prepared.session.memory().clone();
            drop(prepared);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(owned);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_pivot_spill_nested_extrema_and_null_parents_preserve_selected_complete_values() {
    let values = (0..3)
        .flat_map(|pass| {
            (0..257).map(move |index| {
                if pass == 1 || index % 11 == 0 {
                    None
                } else {
                    Some(vec![
                        Some(i64::from(index)),
                        Some(if pass == 0 { 9 } else { 3 }),
                        None,
                    ])
                }
            })
        })
        .collect::<Vec<_>>();
    let fixture = table(
        VarBinArray::from((0..3).flat_map(|_| (0..257).map(key)).collect::<Vec<_>>()).into_array(),
        VarBinArray::from(vec!["a"; values.len()]).into_array(),
        lists(&values),
        71,
    );
    for (aggregate, selected) in [("min", 3), ("max", 9)] {
        let plan = plan(&fixture, aggregate);
        let resident = prepare(&fixture, plan.clone(), false);
        let prepared = prepare(&fixture, plan, true);
        let expected = (0..257).map(|index| json!({
            "entity":key(index), "pivot_a":if index % 11 == 0 { Value::Null } else {json!([index,selected,null])}
        })).collect::<Vec<_>>();
        let (actual, report) = complete(&prepared);
        assert_eq!(actual, expected);
        assert_eq!(complete(&resident).0, expected);
        assert!(report.spill.as_ref().unwrap().merge_passes > 0);
    }

    // Hidden nonfinite children of a NULL struct are not selected or compared.
    let children = (0..2).flat_map(|_| 0..257).collect::<Vec<_>>();
    let nullable = StructArray::new(
        FieldNames::from(["numeric", "fixed"]),
        vec![
            PrimitiveArray::from_iter(children.iter().map(|index| {
                if index % 2 == 0 {
                    f64::NAN
                } else {
                    f64::from(u32::try_from(*index).unwrap())
                }
            }))
            .into_array(),
            FixedSizeListArray::try_new(
                PrimitiveArray::from_iter(
                    children
                        .iter()
                        .flat_map(|index| [i64::try_from(*index).unwrap(), i64::MAX]),
                )
                .into_array(),
                2,
                Validity::NonNullable,
                children.len(),
            )
            .unwrap()
            .into_array(),
        ],
        children.len(),
        Validity::from_iter(children.iter().map(|index| index % 2 != 0)),
    )
    .into_array();
    let fixture = table(
        VarBinArray::from(children.iter().map(|index| key(*index)).collect::<Vec<_>>())
            .into_array(),
        VarBinArray::from(vec!["a"; children.len()]).into_array(),
        nullable,
        43,
    );
    let prepared = prepare(&fixture, plan(&fixture, "first_unique"), true);
    assert_eq!(complete(&prepared).0, (0..257).map(|index| json!({
        "entity":key(index), "pivot_a": if index % 2 == 0 {Value::Null} else {json!({"numeric":f64::from(u32::try_from(index).unwrap()),"fixed":[index,i64::MAX]})},
    })).collect::<Vec<_>>());
}

#[test]
fn native_pivot_spill_nested_index_and_domain_order_names_are_exact() {
    let indices = (0..2)
        .flat_map(|_| (0..257).map(|index| Some(vec![Some(i64::from(index))])))
        .collect::<Vec<_>>();
    let domains = (0..2)
        .flat_map(|_| (0..257).map(|_| Some(vec![Some(7), None])))
        .collect::<Vec<_>>();
    let fixture = table(
        lists(&indices),
        lists(&domains),
        VarBinArray::from((0..2).flat_map(|_| (0..257).map(key)).collect::<Vec<_>>()).into_array(),
        37,
    );
    let plan = plan(&fixture, "first_unique");
    let resident = prepare(&fixture, plan.clone(), false);
    let prepared = prepare(&fixture, plan, true);
    let (actual, report) = complete(&prepared);
    let mut expected = (0..257)
        .map(|index| json!({"entity":[index], "pivot_l2_i7_n":key(index)}))
        .collect::<Vec<_>>();
    // The established pivot order is its serialized exact key, not numeric list order.
    expected.sort_by_key(|row| format!("L1:i{};", row["entity"][0].as_u64().unwrap()));
    assert_eq!(actual, complete(&resident).0);
    assert_eq!(actual, expected);
    assert_eq!(report.spilled_pivot_index_rows, 257);
    assert_eq!(report.spilled_pivot_domains, 1);
}
