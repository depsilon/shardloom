//! FILTER defines each reducer's observation domain, independently of grouping and frames.

use super::*;
use vortex::array::arrays::BoolArray;

#[path = "local_primitive_relational_aggregate_filter_lifecycle_tests.rs"]
mod lifecycle;

fn filtered(mut measure: Measure, name: &str) -> Measure {
    measure.filter_column = Some(ColumnRef::new(name).unwrap());
    measure
}

fn measures() -> Vec<Measure> {
    let mut measures = vec![measure("count", None, "rows")];
    for function in [
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "count_distinct",
        "sum_distinct",
        "avg_distinct",
        "var_pop",
        "var_samp",
        "median",
        "first",
        "last",
        "list",
    ] {
        measures.push(filtered(measure(function, Some("value"), function), "keep"));
    }
    measures.push(filtered(measure("count", None, "admitted"), "keep"));
    for function in ["covar_pop", "covar_samp", "corr"] {
        measures.push(filtered(
            measure(function, Some("value"), function)
                .with_secondary_column(ColumnRef::new("paired").unwrap()),
            "keep",
        ));
    }
    for function in ["arg_min", "arg_max"] {
        measures.push(filtered(
            measure(function, Some("value"), function)
                .with_secondary_column(ColumnRef::new("local_order").unwrap()),
            "keep",
        ));
    }
    measures.extend([
        filtered(
            measure("quantile_disc", Some("value"), "disc")
                .with_quantile_level(0.5)
                .unwrap(),
            "keep",
        ),
        filtered(measure("var_pop", Some("value"), "other_var"), "other"),
        filtered(measure("median", Some("value"), "other_median"), "other"),
        filtered(
            measure("string_agg", Some("text"), "text")
                .with_separator("|")
                .with_order_by(order("local_order", true)),
            "keep",
        ),
    ]);
    measures
}

fn grouped_fixture() -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from([
                "group",
                "value",
                "paired",
                "local_order",
                "text",
                "keep",
                "other",
            ]),
            vec![
                VarBinViewArray::from_iter_str(["A", "A", "A", "A", "A", "A", "B", "B", "C", "C"])
                    .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(1.0),
                    Some(3.0),
                    Some(5.0),
                    Some(9.0),
                    Some(f64::NAN),
                    None,
                    Some(f64::INFINITY),
                    Some(f64::NAN),
                    None,
                    None,
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(3.0),
                    Some(7.0),
                    Some(11.0),
                    Some(19.0),
                    Some(f64::INFINITY),
                    None,
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    None,
                    None,
                ])
                .into_array(),
                PrimitiveArray::from_iter([
                    2.0,
                    1.0,
                    0.0,
                    3.0,
                    f64::NAN,
                    0.0,
                    f64::NAN,
                    f64::INFINITY,
                    1.0,
                    0.0,
                ])
                .into_array(),
                VarBinViewArray::from_iter_nullable_str([
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                    Some("excluded"),
                    None,
                    Some("excluded"),
                    Some("excluded"),
                    None,
                    None,
                ])
                .into_array(),
                BoolArray::from_iter([
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    Some(false),
                    Some(false),
                    None,
                    Some(true),
                    Some(true),
                ])
                .into_array(),
                BoolArray::from_iter([
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(false),
                    None,
                    Some(false),
                    Some(false),
                ])
                .into_array(),
            ],
            10,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

#[test]
fn aggregate_filter_groups_preserve_empty_domains_and_separate_shared_states() {
    let fixture = grouped_fixture();
    let a = json!({"group":"A","rows":6,"count":2,"sum":4.0,"avg":2.0,"min":1.0,"max":3.0,
        "count_distinct":2,"sum_distinct":4.0,"avg_distinct":2.0,"var_pop":1.0,"var_samp":2.0,
        "median":2.0,"first":1.0,"last":3.0,"list":[1.0,3.0],"admitted":2,"covar_pop":2.0,
        "covar_samp":4.0,"corr":1.0,"arg_min":3.0,"arg_max":1.0,"disc":1.0,
        "other_var":4.0,"other_median":7.0,"text":"a"});
    let mut b = a.clone();
    for (name, value) in b.as_object_mut().unwrap() {
        *value = match name.as_str() {
            "group" => json!("B"),
            "rows" => json!(2),
            "count" | "count_distinct" | "admitted" => json!(0),
            _ => Value::Null,
        };
    }
    let mut c = b.clone();
    c["group"] = json!("C");
    c["admitted"] = json!(2);
    c["list"] = json!([null, null]);
    check(
        &fixture,
        &aggregate(fixture.scan(), &["group"], measures()),
        &[a, b, c],
    );
}

#[test]
fn aggregate_filter_windows_apply_after_all_frame_units_and_exclusions() {
    let admitted = [
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(true),
        Some(false),
        Some(true),
        None,
    ];
    let numeric = [
        Some(1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        None,
        Some(5.0),
        Some(f64::NAN),
        Some(5.0),
        Some(f64::NEG_INFINITY),
    ];
    let text = [
        Some("a"),
        Some("bad"),
        Some("bad"),
        None,
        Some("e"),
        Some("bad"),
        Some("g"),
        Some("bad"),
    ];
    let fixture = filtered_window_fixture(admitted, numeric, text);
    for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
        verify_window(unit, &fixture, admitted, numeric, text);
    }
}

fn filtered_window_fixture(
    admitted: [Option<bool>; 8],
    numeric: [Option<f64>; 8],
    text: [Option<&str>; 8],
) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from([
                "id",
                "window_key",
                "value",
                "paired",
                "local_order",
                "text",
                "keep",
            ]),
            vec![
                PrimitiveArray::from_iter(IDS).into_array(),
                PrimitiveArray::from_iter(WINDOW_KEYS).into_array(),
                PrimitiveArray::from_option_iter(numeric).into_array(),
                PrimitiveArray::from_option_iter(
                    numeric.map(|value| value.map(|value| value * 2.0 + 1.0)),
                )
                .into_array(),
                PrimitiveArray::from_option_iter(LOCAL_ORDER).into_array(),
                VarBinViewArray::from_iter_nullable_str(text).into_array(),
                BoolArray::from_iter(admitted).into_array(),
            ],
            IDS.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn verify_window(
    unit: Unit,
    fixture: &Fixture,
    admitted: [Option<bool>; 8],
    numeric: [Option<f64>; 8],
    text: [Option<&str>; 8],
) {
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
        let measures: Vec<_> = vec![
            measure("count", None, "n"),
            measure("sum_distinct", Some("value"), "sum_distinct"),
            measure("var_pop", Some("value"), "variance"),
            measure("covar_pop", Some("value"), "covariance")
                .with_secondary_column(ColumnRef::new("paired").unwrap()),
            measure("quantile_disc", Some("value"), "median")
                .with_quantile_level(0.5)
                .unwrap(),
            measure("first", Some("text"), "first").with_order_by(order("local_order", true)),
            measure("list", Some("value"), "items"),
            measure("string_agg", Some("text"), "text")
                .with_separator("|")
                .with_order_by(order("local_order", false)),
        ]
        .into_iter()
        .map(|measure| filtered(measure, "keep"))
        .collect();
        let expected = IDS.iter().enumerate().map(|(row, &id)| {
                let mut selected = frame_members(unit, exclusion, row).into_iter()
                    .filter(|&row| admitted[row] == Some(true)).collect::<Vec<_>>();
                selected.sort_by_key(|row| PARTITION_ORDER.iter().position(|candidate| candidate == row).unwrap());
                let mut values = selected.iter().filter_map(|&row| numeric[row]).collect::<Vec<_>>();
                values.sort_by(f64::total_cmp);
                let count = f64::from(u32::try_from(values.len()).unwrap());
                let sum = values.iter().sum::<f64>();
                let variance = (!values.is_empty()).then(|| (count * values.iter().map(|value| value * value).sum::<f64>() - sum * sum) / (count * count));
                let median = values.get(values.len().div_ceil(2).saturating_sub(1)).copied();
                values.dedup();
                let sum = (!values.is_empty()).then(|| values.iter().sum::<f64>());
                let first = local_ordered(selected.clone(), true).first().and_then(|&row| text[row]);
                let joined = local_ordered(selected.clone(), false).iter().filter_map(|&row| text[row]).collect::<Vec<_>>();
                json!({"id":id,"n":selected.len(),"sum_distinct":sum,"variance":variance,"covariance":variance.map(|v| v*2.0),
                    "median":median,"first":first,"items":if selected.is_empty() { Value::Null } else { json!(selected.iter().map(|&row| numeric[row]).collect::<Vec<_>>()) },
                    "text":(!joined.is_empty()).then(|| joined.join("|"))})
            }).collect::<Vec<_>>();
        if unit == Unit::Rows && exclusion == Exclusion::NoOthers {
            for measure in &measures {
                let selected = expected
                    .iter()
                    .map(|row| json!({"id":row["id"],measure.alias.clone():row[&measure.alias]}))
                    .collect::<Vec<_>>();
                check(
                    fixture,
                    &window_plan(fixture, &frame, vec![measure.clone()]),
                    &selected,
                );
            }
        }
        check(fixture, &window_plan(fixture, &frame, measures), &expected);
    }
}

#[test]
fn aggregate_filter_rejects_non_boolean_columns_during_binding() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter([1u64]).into_array()),
        1,
    );
    let plan = aggregate(
        fixture.scan(),
        &[],
        vec![filtered(measure("count", None, "n"), "value")],
    );
    let error = prepare_relational(&plan, policy()).err().unwrap();
    assert!(
        error.to_string().contains("FILTER requires a Boolean"),
        "{error}"
    );
}
