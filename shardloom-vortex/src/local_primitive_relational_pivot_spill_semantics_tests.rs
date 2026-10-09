use super::*;
use shardloom_core::ScalarValue;
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

fn decimal_fixture(values: &[i128], chunk: usize) -> Fixture {
    let indices = (0..values.len())
        .flat_map(|_| (0..257).map(key))
        .collect::<Vec<_>>();
    let rows = indices.len();
    table(
        VarBinArray::from(indices).into_array(),
        VarBinArray::from(vec!["a"; rows]).into_array(),
        DecimalArray::from_iter(
            values
                .iter()
                .flat_map(|value| std::iter::repeat_n(*value, 257)),
            DecimalDType::new(38, 6),
        )
        .into_array(),
        chunk,
    )
}

fn configure(
    plan: &mut VortexRelationalPlan,
    change: impl FnOnce(&mut VortexQueryPrimitiveRequest),
) {
    let VortexRelationalPlan::Unary(unary) = plan else {
        panic!("pivot plan required")
    };
    change(&mut unary.request);
}

#[test]
fn native_pivot_spill_decimal_wide_state_cancels_and_averages_before_final_precision() {
    let maximum = 10i128.pow(38) - 1;
    for (aggregate, values, expected) in [
        ("sum", vec![maximum, maximum, -maximum], maximum),
        ("sum", vec![-maximum, -maximum, maximum], -maximum),
        ("mean", vec![maximum, maximum], maximum),
        ("min", vec![maximum, maximum, -maximum], -maximum),
        ("max", vec![-maximum, -maximum, maximum], maximum),
    ] {
        let fixture = decimal_fixture(&values, 59);
        let plan = plan(&fixture, aggregate);
        let resident = prepare(&fixture, plan.clone(), false);
        let prepared = prepare(&fixture, plan, true);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let expected = (0..257)
            .map(|index| {
                json!({
                    "entity":key(index), "pivot_a":format!("decimal128(38,6):{expected}")
                })
            })
            .collect::<Vec<_>>();
        let (actual, report) = complete(&prepared);
        assert_eq!(actual, expected, "{aggregate}");
        assert_eq!(complete(&resident).0, expected);
        assert!(report.spill.as_ref().unwrap().merge_passes > 0);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let fixture = decimal_fixture(&[maximum, maximum], 89);
    let mut plan = plan(&fixture, "mean");
    configure(&mut plan, |request| {
        request.source_order_limit = Some(17);
        let pivot = request.pivot_projection.as_mut().unwrap();
        pivot.margins = true;
        pivot.margins_name = "total".into();
    });
    let resident = prepare(&fixture, plan.clone(), false);
    let prepared = prepare(&fixture, plan, true);
    let value = format!("decimal128(38,6):{maximum}");
    let mut expected = (0..16)
        .map(|index| {
            json!({
                "entity":key(index), "pivot_a":value, "pivot_total":value,
            })
        })
        .collect::<Vec<_>>();
    expected.push(json!({"entity":"total", "pivot_a":value, "pivot_total":value}));
    let (actual, report) = complete(&prepared);
    assert_eq!(actual, expected);
    assert_eq!(complete(&resident).0, expected);
    assert_eq!(report.spilled_pivot_index_rows, 257);
    assert_eq!(
        report.spilled_pivot_reader_opens,
        report.spill.as_ref().unwrap().runs_written + 2
    );
}

#[test]
fn native_pivot_spill_decimal_final_overflow_and_inexactness_release_all_state() {
    let maximum = 10i128.pow(38) - 1;
    for (aggregate, values, message) in [
        ("sum", vec![maximum, maximum], "precision overflow"),
        ("mean", vec![1, 0, 0], "nonzero fractional digits"),
    ] {
        let fixture = decimal_fixture(&values, 71);
        let plan = plan(&fixture, aggregate);
        let resident = prepare(&fixture, plan.clone(), false);
        let prepared = prepare(&fixture, plan, true);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let reference = resident.execute_owned().err().unwrap();
        let mut delivered = 0;
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered += 1;
                Ok(())
            })
            .err()
            .unwrap();
        assert_eq!(error.to_string(), reference.to_string());
        assert!(error.to_string().contains(message));
        assert_eq!(delivered, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("pivot-runs")).unwrap().count(),
            0
        );
    }
}

#[test]
fn native_pivot_spill_sparse_weighted_margins_fill_and_limits_use_selected_indices() {
    let mut indices = Vec::new();
    let mut domains = Vec::new();
    let mut values = Vec::new();
    for (domain, value) in [("a", 1.0), ("b", 2.0), ("a", 4.0)] {
        for index in 0..257 {
            if domain == "b" && index % 2 == 0 {
                continue;
            }
            indices.push(key(index));
            domains.push(domain);
            values.push(value);
        }
    }
    let fixture = table(
        VarBinArray::from(indices).into_array(),
        VarBinArray::from(domains).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
        61,
    );
    for aggregate in ["sum", "mean", "min", "max", "count"] {
        for limit in [None, Some(3), Some(1)] {
            let mut plan = plan(&fixture, aggregate);
            configure(&mut plan, |request| {
                request.source_order_limit = limit;
                let pivot = request.pivot_projection.as_mut().unwrap();
                pivot.margins = true;
                pivot.margins_name = "total".into();
                pivot.fill_value = Some(if aggregate == "count" {
                    ScalarValue::UInt64(0)
                } else {
                    ScalarValue::Float64(0.0)
                });
            });
            let prepared = prepare(&fixture, plan.clone(), true);
            let resident = prepare(&fixture, plan, false);
            let selected = limit.unwrap_or(usize::MAX).saturating_sub(1).min(257);
            let summarize = |values: &[f64]| -> Value {
                if values.is_empty() {
                    return if aggregate == "count" {
                        json!(0)
                    } else {
                        json!(0.0)
                    };
                }
                match aggregate {
                    "count" => json!(values.len()),
                    "sum" => json!(values.iter().sum::<f64>()),
                    "mean" => json!(
                        values.iter().sum::<f64>()
                            / f64::from(u32::try_from(values.len()).unwrap())
                    ),
                    "min" => json!(values.iter().copied().reduce(f64::min).unwrap()),
                    "max" => json!(values.iter().copied().reduce(f64::max).unwrap()),
                    _ => unreachable!(),
                }
            };
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut all = Vec::new();
            let mut expected = Vec::new();
            for index in 0..selected {
                let a_values = [1.0, 4.0];
                let b_values = if index % 2 == 0 { &[][..] } else { &[2.0][..] };
                let mut row = a_values.to_vec();
                row.extend_from_slice(b_values);
                expected.push(json!({"entity":key(index), "pivot_a":summarize(&a_values),
                    "pivot_b":summarize(b_values), "pivot_total":summarize(&row)}));
                a.extend_from_slice(&a_values);
                b.extend_from_slice(b_values);
                all.extend(row);
            }
            expected.push(json!({"entity":"total", "pivot_a":summarize(&a),
                "pivot_b":summarize(&b), "pivot_total":summarize(&all)}));
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let (actual, report) = complete(&prepared);
            assert_eq!(actual, expected, "{aggregate}/{limit:?}");
            assert_eq!(complete(&resident).0, expected);
            assert_eq!(report.spilled_pivot_domains, 2);
            assert_eq!(report.spilled_pivot_index_rows, 257);
            assert_eq!(report.spilled_pivot_cells, 385);
            assert_eq!(
                report.spilled_pivot_reader_opens,
                report.spill.as_ref().unwrap().runs_written + 2
            );
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}
