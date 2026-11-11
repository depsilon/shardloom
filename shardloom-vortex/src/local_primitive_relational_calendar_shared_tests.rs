//! Compatible families across literal DST-fold boundaries and calendar retreat.

use super::*;

fn shared_plan(
    input: VortexRelationalPlan,
    exclusion: Exclusion,
    descending: bool,
    nulls: NullOrder,
) -> VortexRelationalPlan {
    let VortexRelationalPlan::Window(mut window) = plan(
        input,
        &Offset::CalendarDays {
            days: 1,
            ambiguity: Ambiguity::Earlier,
        },
        exclusion,
        descending,
        nulls,
        true,
    ) else {
        panic!("calendar window declaration")
    };
    let shape = window.expressions[0].clone();
    window.expressions = [
        ("sum", "sum"),
        ("avg", "mean"),
        ("count", "nonnull"),
        ("var_pop", "var_pop"),
        ("var_samp", "var_samp"),
        ("stddev_pop", "stddev_pop"),
        ("stddev_samp", "stddev_samp"),
    ]
    .into_iter()
    .map(|(function, alias)| {
        let mut expression = shape.clone();
        expression.output_column = alias.into();
        expression.function = VortexRelationalWindowFunction::Framed(Function::Aggregate(
            Box::new(crate::query_primitive::VortexSimpleAggregateMeasure::new(
                function,
                Some(column("value")),
                alias.into(),
            )),
        ));
        expression
    })
    .collect();
    VortexRelationalPlan::Window(window)
}

fn shared_reference(
    rows: &[Row],
    exclusion: Exclusion,
    descending: bool,
    nulls: NullOrder,
) -> Vec<Value> {
    reference(rows, 1, exclusion, descending, nulls, true)
        .into_iter()
        .map(|mut row| {
            let count = u32::try_from(row["nonnull"].as_u64().unwrap()).unwrap();
            let population = row["var_pop"].as_f64();
            let sample = population.and_then(|value| {
                (count > 1).then(|| value * f64::from(count) / f64::from(count - 1))
            });
            let values = row.as_object_mut().unwrap();
            values.retain(|key, _| {
                matches!(key.as_str(), "id" | "sum" | "mean" | "nonnull" | "var_pop")
            });
            values.insert("var_samp".into(), json!(sample));
            values.insert("stddev_pop".into(), json!(population.map(f64::sqrt)));
            values.insert("stddev_samp".into(), json!(sample.map(f64::sqrt)));
            row
        })
        .collect()
}

fn same_values(actual: &[Value], expected: &[Value]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.as_object().unwrap().len(), 8);
        for name in ["id", "sum", "nonnull"] {
            assert_eq!(actual[name], expected[name]);
        }
        for name in ["mean", "var_pop", "var_samp", "stddev_pop", "stddev_samp"] {
            if let Some(expected) = expected[name].as_f64() {
                let actual = actual[name].as_f64().unwrap();
                assert!(actual.is_finite() && (actual - expected).abs() < 1e-12);
            } else {
                assert_eq!(actual[name], Value::Null);
            }
        }
    }
}

fn shared_schema() -> DType {
    DType::struct_(
        [
            "id",
            "sum",
            "mean",
            "nonnull",
            "var_pop",
            "var_samp",
            "stddev_pop",
            "stddev_samp",
        ]
        .into_iter()
        .map(|name| {
            let dtype = if matches!(name, "id" | "nonnull") {
                DType::Primitive(PType::U64, Nullability::NonNullable)
            } else {
                DType::Primitive(PType::F64, Nullability::Nullable)
            };
            (name.to_owned(), dtype)
        })
        .collect::<Vec<_>>(),
        Nullability::NonNullable,
    )
}

#[test]
fn compatible_window_calendar_retreat_keeps_each_family_and_exclusion_exact() {
    assert!(
        FOLD.windows(2)
            .any(|pair| pair[0].1 > pair[1].1 && pair[0].2 > pair[1].2)
    );
    let mut rows: Vec<_> = FOLD
        .iter()
        .enumerate()
        .map(|(id, &(key, lower, upper))| Row {
            key: Some(key),
            lower: i128::from(lower),
            upper: i128::from(upper),
            group: 0,
            value: (id % 5 != 0).then_some(i64::try_from(id % 7).unwrap() - 3),
        })
        .collect();
    rows.push(rows[8].clone());
    for id in [2, 7, 8, 9, 10, 14] {
        let mut row = rows[id].clone();
        row.group = 1;
        rows.push(row);
    }
    for group in [0, 0, 1] {
        rows.push(Row {
            key: None,
            lower: 0,
            upper: 0,
            group,
            value: Some(9),
        });
    }
    rows.rotate_left(9);
    rows.swap(2, 18);
    let fixture = fixture("timestamp(s,America/New_York)", &rows, 3);
    for descending in [false, true] {
        for nulls in [NullOrder::First, NullOrder::Last] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                let plan = shared_plan(fixture.scan(), exclusion, descending, nulls);
                let wanted = shared_reference(&rows, exclusion, descending, nulls);
                let mut resources =
                    VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
                resources.resource_envelope.memory_budget_bytes = 32 << 20;
                let resident = prepare_relational(&plan, resources).unwrap();
                assert!(resident.provider.is_none());
                let schema = shared_schema();
                assert_eq!(resident.output_dtype().as_ref(), Some(&schema));
                let baseline = resident.snapshot().memory.reserved_bytes;
                #[cfg(debug_assertions)]
                let observation =
                    crate::local_primitives::native_relational_observation::Session::start();
                let result = resident
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                #[cfg(debug_assertions)]
                let counts = observation.finish();
                same_values(&json_rows(&result), &wanted);
                assert!(result.execution.native_io_certificate.is_certified());
                assert!(
                    !result
                        .execution
                        .native_io_certificate
                        .side_effects
                        .fallback_attempted
                );
                assert_eq!(result.execution.ordered_window_stages, 0);
                drop(result);
                assert_eq!(resident.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(resident.snapshot().admitted_max_parallelism, 1);
                assert_eq!(resident.snapshot().provider_background_workers, 0);
                #[cfg(debug_assertions)]
                eprintln!(
                    "WINDOW_SHARED_CALENDAR_WORK {}",
                    json!({"descending":descending, "nulls_first":matches!(nulls, NullOrder::First),
                        "exclusion":format!("{exclusion:?}"), "input_rows":rows.len(),
                        "observations":counts, "complete_values_and_schema_match":true,
                        "credits_refunded":true, "engine_timing_claimed":false})
                );
                let stored = ordered(&fixture, &plan);
                let baseline = stored.snapshot().memory.reserved_bytes;
                let (actual, report) = complete(&stored, 3);
                same_values(&actual, &wanted);
                assert_eq!(stored.output_dtype(), resident.output_dtype());
                drop(report);
                assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
}
